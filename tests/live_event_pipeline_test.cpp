#include "test_framework.h"

#include "hft/csv_output_set.h"
#include "hft/live_event_pipeline.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>

#include <unistd.h>

namespace hft::test {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static unsigned sequence{0U};
        path_ = std::filesystem::temp_directory_path() /
            ("hft_live_pipeline_" +
             std::to_string(::getpid()) + "_" +
             std::to_string(sequence++));
        std::filesystem::create_directory(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_{};
};

[[nodiscard]] std::string read_binary_file(
    const std::filesystem::path& path) {
    std::ifstream input{path, std::ios::binary};
    return std::string{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}};
}

[[nodiscard]] std::size_t line_count(
    const std::string_view bytes) noexcept {
    return static_cast<std::size_t>(
        std::count(bytes.begin(), bytes.end(), '\n'));
}

[[nodiscard]] std::unique_ptr<LiveSubscription>
make_subscription(
    Context& context,
    const std::string_view* const symbols,
    const std::size_t symbol_count) {
    SubscriptionError error;
    std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols,
            symbol_count,
            error);
    context.expect(
        subscription != nullptr,
        "live pipeline subscription is created");
    return subscription;
}

[[nodiscard]] std::unique_ptr<LiveEventPipeline> make_pipeline(
    Context& context,
    const LiveSubscription& subscription) {
    LivePipelineCreateError error;
    std::unique_ptr<LiveEventPipeline> pipeline =
        LiveEventPipeline::create(subscription, error);
    context.expect(
        pipeline != nullptr &&
            error == LivePipelineCreateError::none,
        "live event pipeline is initialized");
    return pipeline;
}

[[nodiscard]] WebSocketTextMessage message(
    const std::uint64_t epoch,
    const std::uint64_t sequence,
    const std::string_view payload,
    const std::uint64_t seconds = 1'700'000'000U,
    const std::uint32_t nanoseconds = 123U) noexcept {
    return WebSocketTextMessage{
        CsvTimestamp{seconds, nanoseconds},
        epoch,
        sequence,
        payload,
    };
}

void test_multi_symbol_output_composition(Context& context) {
    constexpr std::array<std::string_view, 2U> symbols{
        "BTCUSDT", "ETHUSDT"};
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(
            context, symbols.data(), symbols.size());
    if (!subscription) {
        return;
    }
    const std::unique_ptr<LiveEventPipeline> pipeline =
        make_pipeline(context, *subscription);
    if (!pipeline) {
        return;
    }

    TemporaryDirectory parent;
    OutputSetOpenError open_error;
    std::unique_ptr<CsvOutputSet> output =
        CsvOutputSet::open_live(
            (parent.path() / "capture").string(),
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            "2026-07-27",
            open_error);
    context.expect(
        output != nullptr,
        "live pipeline output set is opened");
    if (!output) {
        return;
    }

    EventRowBatch batch;
    constexpr std::string_view btc_refresh =
        R"({"stream":"btcusdt@depth5@100ms","data":{"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","2"]]}})";
    LivePipelineResult result =
        pipeline->process(
            message(0U, 1U, btc_refresh), batch);
    context.expect(
        result.has_batch() &&
            result.target_index == 0U &&
            result.event.status ==
                EventProcessStatus::applied_refresh &&
            batch.has_audit_row &&
            batch.has_order_book_row &&
            !output->write_batch(result.target_index, batch),
        "BTC refresh routes as one audit-plus-book batch");

    constexpr std::string_view eth_trade =
        R"({"stream":"ethusdt@trade","data":{"e":"trade","s":"ETHUSDT","p":"2000","q":"0.5"}})";
    result = pipeline->process(
        message(0U, 2U, eth_trade), batch);
    context.expect(
        result.has_batch() &&
            result.target_index == 1U &&
            result.event.status ==
                EventProcessStatus::trade_audited &&
            batch.has_audit_row &&
            !batch.has_order_book_row &&
            !output->write_batch(result.target_index, batch),
        "ETH trade routes independently as audit-only");

    constexpr std::string_view btc_diff =
        R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100","3"]],"a":[]}})";
    result = pipeline->process(
        message(0U, 3U, btc_diff), batch);
    context.expect(
        result.has_batch() &&
            result.target_index == 0U &&
            result.event.status ==
                EventProcessStatus::applied_diff &&
            batch.has_audit_row &&
            batch.has_order_book_row &&
            !output->write_batch(result.target_index, batch),
        "interleaved BTC diff preserves connection order");

    const std::string btc_audit_path{
        output->audit_path(0U)};
    const std::string btc_book_path{
        output->order_book_path(0U)};
    const std::string eth_audit_path{
        output->audit_path(1U)};
    const std::string eth_book_path{
        output->order_book_path(1U)};
    context.expect(
        !output->close_all(),
        "composed live output closes cleanly");

    const std::string btc_audit =
        read_binary_file(btc_audit_path);
    const std::string btc_book =
        read_binary_file(btc_book_path);
    const std::string eth_audit =
        read_binary_file(eth_audit_path);
    const std::string eth_book =
        read_binary_file(eth_book_path);
    context.expect(
        line_count(btc_audit) == 3U &&
            btc_audit.find(
                ",spot,depth5,0,0,1,BTCUSDT,") !=
                std::string::npos &&
            btc_audit.find(
                ",spot,depth_diff,0,0,3,BTCUSDT,") !=
                std::string::npos &&
            btc_audit.find("ETHUSDT") == std::string::npos,
        "BTC audit file contains its ordered subsequence");
    context.expect(
        line_count(eth_audit) == 2U &&
            eth_audit.find(
                ",spot,trade,0,0,2,ETHUSDT,") !=
                std::string::npos &&
            eth_audit.find("BTCUSDT") == std::string::npos,
        "ETH audit file contains only its routed trade");
    context.expect(
        line_count(btc_book) == 3U &&
            btc_book.find(",1,1747767916,P,N,") !=
                std::string::npos &&
            btc_book.find(",2,1747767916,D,B,") !=
                std::string::npos &&
            eth_book == kOrderBookCsvHeader,
        "book output remains per-file and seqNo-contiguous");

    result = pipeline->process(
        message(0U, 4U, eth_trade), batch);
    const OutputSetWriteError closed_write =
        output->write_batch(result.target_index, batch);
    context.expect(
        result.has_batch() &&
            closed_write.code ==
                OutputSetWriteErrorCode::closed,
        "writer-boundary failure remains explicit after processing");
}

void test_routed_pre_audit_invalidation(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{
        "BTCUSDT"};
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(
            context, symbols.data(), symbols.size());
    if (!subscription) {
        return;
    }
    const std::unique_ptr<LiveEventPipeline> pipeline =
        make_pipeline(context, *subscription);
    if (!pipeline) {
        return;
    }
    EventRowBatch batch;

    constexpr std::string_view refresh =
        R"({"stream":"btcusdt@depth5@100ms","data":{"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","2"]]}})";
    static_cast<void>(pipeline->process(
        message(0U, 1U, refresh), batch));
    context.expect(
        pipeline->state(0U) != nullptr &&
            pipeline->state(0U)->book_valid(),
        "refresh establishes a valid routed book");

    constexpr std::string_view missing_data =
        R"({"stream":"btcusdt@depth@100ms"})";
    LivePipelineResult result = pipeline->process(
        message(0U, 2U, missing_data), batch);
    context.expect(
        result.handled() && !result.has_batch() &&
            result.envelope_error ==
                LiveEnvelopeErrorCode::missing_data &&
            result.route != nullptr &&
            result.target_index == 0U &&
            !pipeline->state(0U)->book_valid() &&
            batch.audit_row.empty() &&
            batch.order_book_row.empty(),
        "missing data invalidates its safely routed diff");

    static_cast<void>(pipeline->process(
        message(0U, 3U, refresh), batch));
    constexpr std::string_view data_before_stream =
        R"({"data":[],"stream":"btcusdt@depth@100ms"})";
    result = pipeline->process(
        message(0U, 4U, data_before_stream), batch);
    context.expect(
        result.disposition ==
                LivePipelineDisposition::pre_audit_rejected &&
            result.envelope_error ==
                LiveEnvelopeErrorCode::data_not_object &&
            result.route != nullptr &&
            !pipeline->state(0U)->book_valid(),
        "data-before-stream rejection remains safely routed");

    static_cast<void>(pipeline->process(
        message(0U, 5U, refresh), batch));
    constexpr std::string_view ambiguous_stream =
        R"({"stream":"btcusdt@depth@100ms","stream":"btcusdt@depth@100ms","data":{}})";
    result = pipeline->process(
        message(0U, 6U, ambiguous_stream), batch);
    context.expect(
        result.envelope_error ==
                LiveEnvelopeErrorCode::duplicate_stream &&
            result.route == nullptr &&
            pipeline->state(0U)->book_valid(),
        "ambiguous duplicate stream cannot invalidate a book");

    constexpr std::string_view malformed_diff =
        R"({"stream":"btcusdt@depth@100ms","data":{"x":]}})";
    result = pipeline->process(
        message(0U, 7U, malformed_diff), batch);
    context.expect(
        result.disposition ==
                LivePipelineDisposition::pre_audit_rejected &&
            result.envelope_error ==
                LiveEnvelopeErrorCode::none &&
            result.event.status ==
                EventProcessStatus::pre_audit_rejected &&
            result.event.parse_error ==
                SpotParseError::malformed_json &&
            !pipeline->state(0U)->book_valid(),
        "malformed routed diff payload is rejected and invalidates");
}

void test_metadata_and_epoch_contract(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{
        "BTCUSDT"};
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(
            context, symbols.data(), symbols.size());
    if (!subscription) {
        return;
    }
    const std::unique_ptr<LiveEventPipeline> pipeline =
        make_pipeline(context, *subscription);
    if (!pipeline) {
        return;
    }
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"stream":"btcusdt@depth5@100ms","data":{"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","2"]]}})";

    LivePipelineResult result = pipeline->process(
        message(1U, 2U, refresh), batch);
    context.expect(
        result.has_batch() &&
            pipeline->state(0U)->book_valid() &&
            pipeline->state(0U)->current_connection_epoch() == 1U &&
            pipeline->state(0U)->last_connection_sequence() == 2U,
        "first observed text may follow an earlier session and sequence");

    result = pipeline->process(
        message(1U, 2U, refresh), batch);
    context.expect(
        result.fatal() &&
            result.error ==
                LivePipelineErrorCode::invalid_message_metadata &&
            batch.audit_row.empty(),
        "duplicate connection sequence is fatal before parsing");

    constexpr std::string_view trade =
        R"({"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","p":"100","q":"1"}})";
    result = pipeline->process(
        message(2U, 1U, trade), batch);
    context.expect(
        result.has_batch() &&
            result.event.status ==
                EventProcessStatus::trade_audited &&
            !pipeline->state(0U)->book_valid(),
        "new epoch invalidates all books before audit-only trade");

    result = pipeline->process(
        message(1U, 3U, trade), batch);
    context.expect(
        result.fatal() &&
            result.error ==
                LivePipelineErrorCode::invalid_message_metadata,
        "older epoch cannot re-enter the pipeline");

    pipeline->invalidate_all();
    context.expect(
        !pipeline->state(0U)->book_valid() &&
            pipeline->state(1U) == nullptr,
        "explicit connection teardown invalidates bounded states");
}

void test_usdm_route_and_sequence_dispatch(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{
        "BTCUSDT"};
    const std::unique_ptr<LiveSubscription> subscription =
        [&]() {
            SubscriptionError error;
            std::unique_ptr<LiveSubscription> value =
                LiveSubscription::create(
                    PayloadVenue::usdm,
                    symbols.data(),
                    symbols.size(),
                    error);
            context.expect(
                value != nullptr,
                "USD-M live pipeline subscription is created");
            return value;
        }();
    if (!subscription) {
        return;
    }
    const std::unique_ptr<LiveEventPipeline> pipeline =
        make_pipeline(context, *subscription);
    if (!pipeline) {
        return;
    }

    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"stream":"btcusdt@depth5@100ms","data":{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":90,"u":100,"pu":89,"b":[["100","1"]],"a":[["101","1"]]}})";
    LivePipelineResult result = pipeline->process(
        message(0U, 1U, refresh), batch);
    context.expect(
        result.has_batch() &&
            result.event.status ==
                EventProcessStatus::applied_refresh &&
            batch.has_audit_row &&
            batch.has_order_book_row &&
            batch.audit_row.view().find(",usdm,depth5,") !=
                std::string_view::npos,
        "USD-M refresh crosses the envelope-to-book boundary");

    constexpr std::string_view bridge =
        R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":100,"u":102,"pu":7,"b":[["100","2"]],"a":[]}})";
    result = pipeline->process(
        message(0U, 2U, bridge), batch);
    context.expect(
        result.has_batch() &&
            result.event.status ==
                EventProcessStatus::applied_diff &&
            pipeline->state(0U) != nullptr &&
            pipeline->state(0U)->sequence_number() == 2U,
        "USD-M diff uses its independent bridge semantics");
}

}  // namespace

void run_live_event_pipeline_tests(Context& context) {
    test_multi_symbol_output_composition(context);
    test_routed_pre_audit_invalidation(context);
    test_metadata_and_epoch_contract(context);
    test_usdm_route_and_sequence_dispatch(context);
}

}  // namespace hft::test
