#include "test_framework.h"

#include "hft/event_processor.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedEventJson {
public:
    explicit PaddedEventJson(const std::string_view json)
        : storage_(json.size() + kJsonPaddingBytes, '\0'),
          size_(json.size()) {
        std::memcpy(storage_.data(), json.data(), json.size());
    }

    [[nodiscard]] PaddedJsonView view() const noexcept {
        return PaddedJsonView{
            storage_.data(), size_, storage_.size()};
    }

private:
    std::vector<char> storage_;
    std::size_t size_;
};

[[nodiscard]] EventContext event_context(
    const PayloadVenue venue,
    const SpotStreamKind kind,
    const std::uint64_t epoch,
    const std::uint64_t connection_sequence,
    const std::string_view symbol = "BTCUSDT") noexcept {
    return EventContext{
        CsvTimestamp{1'700'000'000, 123'456'789},
        venue,
        kind,
        0U,
        epoch,
        connection_sequence,
        symbol,
    };
}

[[nodiscard]] EventProcessResult process(
    EventProcessor& processor,
    SymbolState& state,
    EventRowBatch& batch,
    const EventContext& context,
    const std::string_view json,
    const EventOutputMode mode = EventOutputMode::live_capture) {
    const PaddedEventJson padded{json};
    return processor.process(
        state, context, padded.view(), mode, batch);
}

[[nodiscard]] bool contains(
    const std::string_view text,
    const std::string_view expected) noexcept {
    return text.find(expected) != std::string_view::npos;
}

[[nodiscard]] std::optional<SymbolState> make_state(
    Context& context,
    const PayloadVenue venue) {
    std::optional<SymbolState> state =
        SymbolState::create(venue, "BTCUSDT");
    context.expect(
        state.has_value(),
        "valid symbol state is created");
    return state;
}

void test_spot_rows_and_sequence(Context& context) {
    std::optional<SymbolState> state = make_state(
        context, PayloadVenue::spot);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;

    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"],["99","2"]],"asks":[["101","3"]]})";
    EventProcessResult result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 0U, 1U),
        refresh);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::applied_refresh &&
            result.book_status == BookApplyStatus::applied_refresh,
        "Spot refresh is parsed, applied, and classified");
    context.expect(
        batch.has_audit_row && batch.has_order_book_row,
        "applied live refresh produces paired rows");
    context.expect(
        contains(
            batch.audit_row.view(),
            "spot,depth5,0,0,1,BTCUSDT,"
            "\"{\"\"lastUpdateId\"\":100"),
        "audit row retains routed context and escaped payload");
    context.expect(
        contains(
            batch.order_book_row.view(),
            ",1,1747767916,P,N,"),
        "first order-book row uses stable ID and one-based seqNo");
    context.expect(
        state->sequence_number() == 1U && state->book_valid(),
        "accepted refresh commits sequence and valid book");

    constexpr std::string_view trade =
        R"({"e":"trade","s":"BTCUSDT","p":"100","q":"1"})";
    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 0U, 2U),
        trade);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::trade_audited &&
            batch.has_audit_row && !batch.has_order_book_row,
        "trade is audit-only");
    context.expect(
        state->sequence_number() == 1U && state->book_valid(),
        "trade does not consume seqNo or mutate book validity");

    constexpr std::string_view diff =
        R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100","2"]],"a":[]})";
    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 0U, 3U),
        diff);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::applied_diff &&
            batch.has_audit_row && batch.has_order_book_row,
        "contiguous Spot diff produces paired rows");
    context.expect(
        contains(
            batch.order_book_row.view(),
            ",2,1747767916,D,B,"),
        "applied diff increments per-file seqNo contiguously");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 0U, 4U),
        diff);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::stale_diff &&
            batch.has_audit_row && !batch.has_order_book_row,
        "stale diff remains audited without a snapshot");
    context.expect(
        state->sequence_number() == 2U,
        "rejected events do not consume seqNo");
}

void test_schema_failure_policy(Context& context) {
    std::optional<SymbolState> state = make_state(
        context, PayloadVenue::spot);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";
    static_cast<void>(process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 0U, 1U),
        refresh));

    constexpr std::string_view malformed_refresh =
        R"({"lastUpdateId":101,"bids":[["100","2"]]})";
    EventProcessResult result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 0U, 2U),
        malformed_refresh);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::schema_rejected &&
            result.parse_error == SpotParseError::missing_field &&
            batch.has_audit_row && !batch.has_order_book_row,
        "schema-invalid refresh is audited without a snapshot");
    context.expect(
        state->book_valid() && state->last_update_id() == 100U,
        "schema-invalid independent refresh preserves prior book");

    constexpr std::string_view malformed_diff =
        R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[]})";
    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 0U, 3U),
        malformed_diff);
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::schema_rejected &&
            batch.has_audit_row && !batch.has_order_book_row,
        "schema-invalid differential is still auditable");
    context.expect(
        !state->book_valid(),
        "schema-invalid differential invalidates routed book");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 0U, 4U),
        R"({"e":"depthUpdate")");
    context.expect(
        result.success() &&
            result.status == EventProcessStatus::pre_audit_rejected &&
            result.parse_error == SpotParseError::malformed_json &&
            !batch.has_audit_row && !batch.has_order_book_row,
        "syntactically invalid payload produces no row");
    context.expect(
        !state->book_valid(),
        "unreadable differential leaves routed book invalid");

    static_cast<void>(process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 0U, 5U),
        refresh));
    context.expect(
        state->book_valid(),
        "later valid refresh recovers after malformed differential");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 0U, 6U),
        R"({"e":"trade")");
    context.expect(
        result.status == EventProcessStatus::pre_audit_rejected &&
            !batch.has_audit_row && state->book_valid(),
        "malformed audit-only trade preserves book validity");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 0U, 7U),
        R"({"e":7,"s":"ETHUSDT"})");
    context.expect(
        result.status == EventProcessStatus::trade_audited &&
            result.trade_diagnostic_mismatch &&
            batch.has_audit_row && !batch.has_order_book_row &&
            state->book_valid(),
        "trade discriminator mismatch remains auditable and non-mutating");
}

void test_context_and_epoch_policy(Context& context) {
    std::optional<SymbolState> state = make_state(
        context, PayloadVenue::spot);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";

    EventContext zero_sequence = event_context(
        PayloadVenue::spot, SpotStreamKind::trade, 0U, 0U);
    EventProcessResult result = process(
        processor, *state, batch, zero_sequence, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context &&
            !state->has_observed_connection() &&
            !batch.has_audit_row,
        "zero conn_seq is rejected before state observation");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 2U, 5U),
        refresh);
    context.expect(
        result.status == EventProcessStatus::applied_refresh &&
            state->current_connection_epoch() == 2U &&
            state->last_connection_sequence() == 5U,
        "first symbol event may begin at a later epoch and sequence");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 2U, 5U),
        "{}");
    context.expect(
        result.error == EventProcessError::invalid_context &&
            !batch.has_audit_row && state->book_valid(),
        "non-increasing conn_seq in one epoch is rejected at source boundary");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 1U, 6U),
        "{}");
    context.expect(
        result.error == EventProcessError::invalid_context &&
            state->current_connection_epoch() == 2U,
        "decreasing epoch is rejected without state mutation");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 3U, 1U),
        "{}");
    context.expect(
        result.status == EventProcessStatus::trade_audited &&
            !state->book_valid() &&
            state->sequence_number() == 1U &&
            state->current_connection_epoch() == 3U,
        "new epoch invalidates book but does not reset per-file seqNo");

    constexpr std::string_view diff =
        R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[],"a":[]})";
    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 3U, 2U),
        diff);
    context.expect(
        result.status ==
                EventProcessStatus::ignored_while_invalid &&
            batch.has_audit_row && !batch.has_order_book_row,
        "diff is rejected until a refresh after reconnect");

    EventContext wrong_venue = event_context(
        PayloadVenue::usdm, SpotStreamKind::trade, 3U, 3U);
    result = process(
        processor, *state, batch, wrong_venue, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "context venue must match symbol-state venue");

    EventContext wrong_symbol = event_context(
        PayloadVenue::spot,
        SpotStreamKind::trade,
        3U,
        3U,
        "ETHUSDT");
    result = process(
        processor, *state, batch, wrong_symbol, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "context symbol must match routed symbol state");

    EventContext invalid_timestamp = event_context(
        PayloadVenue::spot, SpotStreamKind::trade, 3U, 3U);
    invalid_timestamp.timestamp.nanoseconds = 1'000'000'000;
    result = process(
        processor, *state, batch, invalid_timestamp, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "overflow receive nanoseconds are rejected at context boundary");
    invalid_timestamp.timestamp.nanoseconds = -1;
    result = process(
        processor, *state, batch, invalid_timestamp, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "negative receive nanoseconds are rejected at context boundary");

    EventContext invalid_shard = event_context(
        PayloadVenue::spot, SpotStreamKind::trade, 3U, 3U);
    invalid_shard.shard_id = 1U;
    result = process(
        processor, *state, batch, invalid_shard, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "baseline event processor rejects nonzero shard IDs");

    EventContext invalid_kind = event_context(
        PayloadVenue::spot, SpotStreamKind::trade, 3U, 3U);
    invalid_kind.stream_kind =
        static_cast<SpotStreamKind>(255U);
    result = process(
        processor, *state, batch, invalid_kind, "{}");
    context.expect(
        result.error == EventProcessError::invalid_context,
        "invalid stream kind is rejected at context boundary");

    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::spot, SpotStreamKind::trade, 3U, 3U),
        "{}",
        static_cast<EventOutputMode>(255U));
    context.expect(
        result.error == EventProcessError::invalid_context,
        "invalid output mode is rejected at context boundary");
}

void test_live_replay_equivalence(Context& context) {
    std::optional<SymbolState> live = make_state(
        context, PayloadVenue::spot);
    std::optional<SymbolState> replay = make_state(
        context, PayloadVenue::spot);
    if (!live || !replay) {
        return;
    }
    EventProcessor live_processor;
    EventProcessor replay_processor;
    EventRowBatch live_batch;
    EventRowBatch replay_batch;
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";
    const EventContext context_value = event_context(
        PayloadVenue::spot, SpotStreamKind::depth5, 0U, 1U);

    const EventProcessResult live_result = process(
        live_processor,
        *live,
        live_batch,
        context_value,
        refresh,
        EventOutputMode::live_capture);
    const EventProcessResult replay_result = process(
        replay_processor,
        *replay,
        replay_batch,
        context_value,
        refresh,
        EventOutputMode::replay);

    context.expect(
        live_result.status == replay_result.status &&
            live_batch.has_audit_row &&
            !replay_batch.has_audit_row,
        "replay suppresses only audit-row emission");
    context.expect(
        live_batch.has_order_book_row &&
            replay_batch.has_order_book_row &&
            live_batch.order_book_row.view() ==
                replay_batch.order_book_row.view(),
        "fixed live and replay inputs produce byte-identical book rows");
}

void test_usdm_dispatch(Context& context) {
    std::optional<SymbolState> state = make_state(
        context, PayloadVenue::usdm);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":90,"u":100,"pu":89,"b":[["100","1"]],"a":[["101","1"]]})";
    EventProcessResult result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::usdm, SpotStreamKind::depth5, 0U, 1U),
        refresh);
    context.expect(
        result.status == EventProcessStatus::applied_refresh &&
            contains(batch.audit_row.view(), ",usdm,depth5,") &&
            contains(
                batch.order_book_row.view(),
                ",1,1747767916,P,N,"),
        "USD-M depth5 dispatches to replacement state");

    constexpr std::string_view bridge =
        R"({"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":100,"u":102,"pu":7,"b":[["100","2"]],"a":[]})";
    result = process(
        processor,
        *state,
        batch,
        event_context(
            PayloadVenue::usdm, SpotStreamKind::depth_diff, 0U, 2U),
        bridge);
    context.expect(
        result.status == EventProcessStatus::applied_diff &&
            state->sequence_number() == 2U,
        "USD-M diff dispatch uses independent bridge policy");
}

void test_checked_counter(Context& context) {
    std::uint64_t next{0};
    context.expect(
        checked_next_sequence(0U, next) && next == 1U,
        "seqNo starts at one");
    context.expect(
        checked_next_sequence(
            std::numeric_limits<std::uint64_t>::max() - 1U,
            next) &&
            next == std::numeric_limits<std::uint64_t>::max(),
        "last representable seqNo is accepted");
    next = 42U;
    context.expect(
        !checked_next_sequence(
            std::numeric_limits<std::uint64_t>::max(), next) &&
            next == 42U,
        "seqNo overflow is rejected without modifying output");
}

}  // namespace

void run_event_processor_tests(Context& context) {
    test_spot_rows_and_sequence(context);
    test_schema_failure_policy(context);
    test_context_and_epoch_policy(context);
    test_live_replay_equivalence(context);
    test_usdm_dispatch(context);
    test_checked_counter(context);
}

}  // namespace hft::test
