#include "test_framework.h"

#include "hft/csv_output_set.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unistd.h>

namespace hft::test {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static unsigned sequence{0U};
        path_ = std::filesystem::temp_directory_path() /
            ("hft_output_set_" + std::to_string(::getpid()) + "_" +
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

class TrackingFileOperations final : public FileOperations {
public:
    [[nodiscard]] FileOpenResult open_exclusive(
        const std::string& path) noexcept override {
        ++open_calls;
        if (open_calls == fail_open_call) {
            return FileOpenResult{-1, open_error};
        }
        const int descriptor = next_descriptor++;
        path_by_descriptor.emplace(descriptor, path);
        output_by_descriptor.emplace(descriptor, std::string{});
        opened_paths.push_back(path);
        return FileOpenResult{descriptor, 0};
    }

    [[nodiscard]] FileWriteResult write(
        const int descriptor,
        const char* const data,
        const std::size_t size) noexcept override {
        write_order.push_back(descriptor);
        if (descriptor == fail_write_descriptor &&
            fail_next_write) {
            fail_next_write = false;
            return FileWriteResult{-1, write_error};
        }
        output_by_descriptor[descriptor].append(data, size);
        return FileWriteResult{
            static_cast<std::ptrdiff_t>(size), 0};
    }

    [[nodiscard]] FileCloseResult close(
        const int descriptor) noexcept override {
        closed_descriptors.push_back(descriptor);
        if (close_error_descriptors.count(descriptor) != 0U) {
            return FileCloseResult{false, close_error};
        }
        return FileCloseResult{true, 0};
    }

    [[nodiscard]] FileRemoveResult remove(
        const std::string& path) noexcept override {
        removed_paths.push_back(path);
        if (fail_remove_paths.count(path) != 0U) {
            return FileRemoveResult{false, remove_error};
        }
        return FileRemoveResult{true, 0};
    }

    [[nodiscard]] int descriptor_for(
        const std::string_view path) const {
        for (const auto& item : path_by_descriptor) {
            if (item.second == path) {
                return item.first;
            }
        }
        return -1;
    }

    [[nodiscard]] const std::string& output_for(
        const int descriptor) const {
        return output_by_descriptor.at(descriptor);
    }

    std::size_t open_calls{0U};
    std::size_t fail_open_call{
        static_cast<std::size_t>(-1)};
    int open_error{EMFILE};
    int next_descriptor{100};
    int fail_write_descriptor{-1};
    bool fail_next_write{false};
    int write_error{ENOSPC};
    int close_error{EIO};
    int remove_error{EACCES};
    std::set<int> close_error_descriptors{};
    std::set<std::string> fail_remove_paths{};
    std::map<int, std::string> path_by_descriptor{};
    std::map<int, std::string> output_by_descriptor{};
    std::vector<std::string> opened_paths{};
    std::vector<std::string> removed_paths{};
    std::vector<int> write_order{};
    std::vector<int> closed_descriptors{};
};

[[nodiscard]] std::string read_binary_file(
    const std::filesystem::path& path) {
    std::ifstream input{path, std::ios::binary};
    return std::string{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}};
}

void fill_audit_row(
    EventRowBatch& batch,
    const std::string_view symbol,
    const std::string_view payload = "{}") {
    batch.clear();
    const MarketDataCsvRow row{
        CsvTimestamp{1'700'000'000, 123},
        PayloadVenue::spot,
        SpotStreamKind::trade,
        0U,
        0U,
        1U,
        symbol,
        payload,
    };
    if (format_market_data_csv_row(row, batch.audit_row) ==
        CsvFormatError::none) {
        batch.has_audit_row = true;
    }
}

void fill_book_row(EventRowBatch& batch) {
    constexpr std::array<BookLevel, 1U> bids{{
        BookLevel{10'000'000'000LL, 200'000'000LL},
    }};
    constexpr std::array<BookLevel, 1U> asks{{
        BookLevel{10'100'000'000LL, 300'000'000LL},
    }};
    const OrderBookCsvRow row{
        CsvTimestamp{1'700'000'000, 123},
        1U,
        123'456,
        BookRowType::partial_refresh,
        BookRowSide::neutral,
        BookSideView{bids.data(), bids.size()},
        BookSideView{asks.data(), asks.size()},
    };
    if (format_order_book_csv_row(row, batch.order_book_row) ==
        CsvFormatError::none) {
        batch.has_order_book_row = true;
    }
}

void test_live_initialization_routing_and_exact_files(Context& context) {
    TemporaryDirectory parent;
    const auto output_directory = parent.path() / "capture";
    constexpr std::array<std::string_view, 2U> symbols{
        "BTCUSDT", "ETHUSDT"};

    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_live(
        output_directory.string(),
        PayloadVenue::spot,
        symbols.data(),
        symbols.size(),
        "2024-02-29",
        open_error);
    context.expect(
        output != nullptr && !open_error &&
            output->mode() == CsvOutputMode::live_capture &&
            output->venue() ==
                std::optional<PayloadVenue>{
                    PayloadVenue::spot} &&
            output->target_count() == 2U,
        "live output set creates every target before returning");
    if (!output) {
        return;
    }

    context.expect(
        output->find_target("BTCUSDT") ==
                std::optional<std::size_t>{0U} &&
            output->find_target("ETHUSDT") ==
                std::optional<std::size_t>{1U} &&
            !output->find_target("BNBUSDT"),
        "normalized symbols resolve to stable target indices");

    const auto btc_audit = output_directory /
        "market_data_spot_BTCUSDT_2024-02-29.csv";
    const auto btc_book = output_directory /
        "market_data_spot_BTCUSDT_2024-02-29_orderbook.csv";
    const auto eth_audit = output_directory /
        "market_data_spot_ETHUSDT_2024-02-29.csv";
    const auto eth_book = output_directory /
        "market_data_spot_ETHUSDT_2024-02-29_orderbook.csv";
    context.expect(
        output->audit_path(0U) == btc_audit.string() &&
            output->order_book_path(0U) == btc_book.string() &&
            output->audit_path(1U) == eth_audit.string() &&
            output->order_book_path(1U) == eth_book.string(),
        "live filenames follow the exact venue-symbol-start-date contract");

    EventRowBatch btc_batch;
    fill_audit_row(btc_batch, "BTCUSDT");
    fill_book_row(btc_batch);
    const std::string btc_audit_row{btc_batch.audit_row.view()};
    const std::string btc_book_row{btc_batch.order_book_row.view()};
    context.expect(
        !output->write_batch(0U, btc_batch),
        "live batch routes to the selected symbol");

    EventRowBatch eth_batch;
    fill_audit_row(eth_batch, "ETHUSDT");
    const std::string eth_audit_row{eth_batch.audit_row.view()};
    context.expect(
        !output->write_batch(1U, eth_batch) &&
            !output->close_all() && output->closed(),
        "audit-only trade batch and checked close succeed");

    context.expect(
        read_binary_file(btc_audit) ==
                std::string{kMarketDataCsvHeader} + btc_audit_row &&
            read_binary_file(btc_book) ==
                std::string{kOrderBookCsvHeader} + btc_book_row &&
            read_binary_file(eth_audit) ==
                std::string{kMarketDataCsvHeader} + eth_audit_row &&
            read_binary_file(eth_book) ==
                std::string{kOrderBookCsvHeader},
        "each live row reaches only its symbol and file kind");
    context.expect(
        output->write_batch(0U, btc_batch).code ==
            OutputSetWriteErrorCode::closed,
        "writes after checked close fail explicitly");
}

void test_replay_book_only_contract(Context& context) {
    TemporaryDirectory parent;
    const auto output_directory = parent.path() / "replay";
    constexpr std::array<ReplayOutputSpec, 2U> specifications{{
        ReplayOutputSpec{
            "BTCUSDT",
            "market_data_spot_BTCUSDT_fixture"},
        ReplayOutputSpec{
            "ETHUSDT",
            "market_data_spot_ETHUSDT_fixture"},
    }};

    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_replay(
        output_directory.string(),
        specifications.data(),
        specifications.size(),
        open_error);
    context.expect(
        output != nullptr && !open_error &&
            output->mode() == CsvOutputMode::replay &&
            !output->venue().has_value() &&
            output->audit_path(0U).empty(),
        "replay output set owns no audit files");
    if (!output) {
        return;
    }

    EventRowBatch batch;
    fill_book_row(batch);
    const std::string expected_row{batch.order_book_row.view()};
    context.expect(
        !output->write_batch(1U, batch) &&
            !output->close_all(),
        "book-only replay batch routes successfully");
    const auto expected_path = output_directory /
        "market_data_spot_ETHUSDT_fixture_orderbook.csv";
    context.expect(
        read_binary_file(expected_path) ==
            std::string{kOrderBookCsvHeader} + expected_row,
        "replay filename and bytes derive from the input stem");

    TemporaryDirectory invalid_parent;
    auto operations = std::make_shared<TrackingFileOperations>();
    OutputSetOpenError invalid_open_error;
    auto invalid_output = CsvOutputSet::open_replay(
        (invalid_parent.path() / "invalid").string(),
        specifications.data(),
        1U,
        operations,
        invalid_open_error);
    if (!invalid_output) {
        context.expect(false, "replay contract test output opens");
        return;
    }
    EventRowBatch invalid_batch;
    fill_audit_row(invalid_batch, "BTCUSDT");
    context.expect(
        invalid_output->write_batch(0U, invalid_batch).code ==
            OutputSetWriteErrorCode::invalid_batch,
        "replay rejects an audit row before file mutation");
    static_cast<void>(invalid_output->close_all());
}

void test_preflight_validation(Context& context) {
    TemporaryDirectory parent;
    const auto nonempty = parent.path() / "nonempty";
    std::filesystem::create_directory(nonempty);
    {
        std::ofstream marker{nonempty / "keep.txt"};
        marker << "keep";
    }
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};

    auto operations = std::make_shared<TrackingFileOperations>();
    OutputSetOpenError nonempty_error;
    auto rejected = CsvOutputSet::open_live(
        nonempty.string(),
        PayloadVenue::spot,
        symbols.data(),
        symbols.size(),
        "2026-07-26",
        operations,
        nonempty_error);
    context.expect(
        !rejected &&
            nonempty_error.code ==
                OutputSetOpenErrorCode::output_directory_not_empty &&
            operations->open_calls == 0U &&
            read_binary_file(nonempty / "keep.txt") == "keep",
        "nonempty output directory is rejected before any file open");

    OutputSetOpenError date_error;
    rejected = CsvOutputSet::open_live(
        (parent.path() / "date").string(),
        PayloadVenue::spot,
        symbols.data(),
        symbols.size(),
        "2025-02-29",
        operations,
        date_error);
    context.expect(
        !rejected &&
            date_error.code ==
                OutputSetOpenErrorCode::invalid_utc_date &&
            operations->open_calls == 0U,
        "invalid calendar date is rejected before output creation");

    constexpr std::array<ReplayOutputSpec, 1U> traversal{{
        ReplayOutputSpec{"BTCUSDT", "../outside"},
    }};
    OutputSetOpenError traversal_error;
    auto replay = CsvOutputSet::open_replay(
        (parent.path() / "traversal").string(),
        traversal.data(),
        traversal.size(),
        operations,
        traversal_error);
    context.expect(
        !replay &&
            traversal_error.code ==
                OutputSetOpenErrorCode::invalid_replay_stem,
        "replay stem cannot escape the selected output directory");
}

void test_maximum_live_target_capacity(Context& context) {
    TemporaryDirectory parent;
    std::array<std::string, kMaxConfiguredSymbols> storage{};
    std::array<std::string_view, kMaxConfiguredSymbols> symbols{};
    for (std::size_t index = 0; index < symbols.size(); ++index) {
        storage[index] = "S" + std::to_string(index);
        symbols[index] = storage[index];
    }
    auto operations = std::make_shared<TrackingFileOperations>();
    OutputSetOpenError error;
    auto output = CsvOutputSet::open_live(
        (parent.path() / "maximum").string(),
        PayloadVenue::usdm,
        symbols.data(),
        symbols.size(),
        "2026-07-26",
        operations,
        error);
    context.expect(
        output != nullptr && !error &&
            output->target_count() == kMaxConfiguredSymbols &&
            operations->open_calls ==
                kMaxConfiguredSymbols * 2U &&
            output->find_target("S31") ==
                std::optional<std::size_t>{31U},
        "maximum baseline symbol set opens exactly 64 stable file targets");
    if (output) {
        static_cast<void>(output->close_all());
    }
}

void test_initialization_rollback(Context& context) {
    TemporaryDirectory parent;
    const auto output_directory = parent.path() / "rollback";
    constexpr std::array<std::string_view, 2U> symbols{
        "BTCUSDT", "ETHUSDT"};
    auto operations = std::make_shared<TrackingFileOperations>();
    operations->fail_open_call = 4U;

    OutputSetOpenError error;
    auto output = CsvOutputSet::open_live(
        output_directory.string(),
        PayloadVenue::usdm,
        symbols.data(),
        symbols.size(),
        "2026-07-26",
        operations,
        error);
    context.expect(
        !output &&
            error.code == OutputSetOpenErrorCode::file_sink_failure &&
            error.target_index == 1U &&
            error.file_kind == CsvFileKind::order_book &&
            error.file_error.code == FileSinkErrorCode::open_failed &&
            error.file_error.native_error == EMFILE,
        "later file-open failure retains exact target and sink context");
    context.expect(
        operations->opened_paths.size() == 3U &&
            operations->closed_descriptors.size() == 3U &&
            operations->removed_paths.size() == 3U &&
            !std::filesystem::exists(output_directory),
        "failed initialization closes and removes every created file and directory");
    context.expect(
        !error.rollback_error,
        "successful rollback does not report a cleanup failure");
}

void test_rollback_failure_is_observable(Context& context) {
    constexpr std::array<std::string_view, 2U> symbols{
        "BTCUSDT", "ETHUSDT"};

    {
        TemporaryDirectory parent;
        const auto output_directory = parent.path() / "remove-failure";
        auto operations =
            std::make_shared<TrackingFileOperations>();
        operations->fail_open_call = 3U;
        const std::string audit_path =
            (output_directory /
             "market_data_spot_BTCUSDT_2026-07-26.csv")
                .string();
        operations->fail_remove_paths.insert(audit_path);

        OutputSetOpenError error;
        auto output = CsvOutputSet::open_live(
            output_directory.string(),
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            "2026-07-26",
            operations,
            error);
        context.expect(
            !output &&
                error.code ==
                    OutputSetOpenErrorCode::file_sink_failure &&
                error.rollback_error.operation ==
                    OutputRollbackOperation::remove_file &&
                error.rollback_error.target_index == 0U &&
                error.rollback_error.file_kind ==
                    CsvFileKind::market_data &&
                error.rollback_error.native_error == EACCES,
            "unlink failure is reported alongside the initiating open failure");
    }

    {
        TemporaryDirectory parent;
        const auto output_directory = parent.path() / "close-failure";
        auto operations =
            std::make_shared<TrackingFileOperations>();
        operations->fail_open_call = 3U;
        operations->close_error_descriptors.insert(101);

        OutputSetOpenError error;
        auto output = CsvOutputSet::open_live(
            output_directory.string(),
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            "2026-07-26",
            operations,
            error);
        context.expect(
            !output &&
                error.rollback_error.operation ==
                    OutputRollbackOperation::close_file &&
                error.rollback_error.target_index == 0U &&
                error.rollback_error.file_kind ==
                    CsvFileKind::order_book &&
                error.rollback_error.file_error.code ==
                    FileSinkErrorCode::close_failed &&
                operations->closed_descriptors.size() == 2U &&
                operations->removed_paths.size() == 2U,
            "rollback continues after close failure and preserves its context");
    }
}

void test_audit_before_book_and_first_failure(Context& context) {
    TemporaryDirectory parent;
    const auto output_directory = parent.path() / "failure";
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    auto operations = std::make_shared<TrackingFileOperations>();

    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_live(
        output_directory.string(),
        PayloadVenue::spot,
        symbols.data(),
        symbols.size(),
        "2026-07-26",
        operations,
        open_error);
    if (!output) {
        context.expect(false, "write-failure output set opens");
        return;
    }

    const int audit_descriptor =
        operations->descriptor_for(output->audit_path(0U));
    const int book_descriptor =
        operations->descriptor_for(output->order_book_path(0U));
    operations->fail_write_descriptor = audit_descriptor;
    operations->fail_next_write = true;

    std::string payload(kCsvAggregationBufferBytes + 100U, 'x');
    EventRowBatch batch;
    fill_audit_row(batch, "BTCUSDT", payload);
    fill_book_row(batch);
    const OutputSetWriteError write_error =
        output->write_batch(0U, batch);
    context.expect(
        write_error.code == OutputSetWriteErrorCode::file_failure &&
            write_error.target_index == 0U &&
            write_error.file_kind == CsvFileKind::market_data &&
            write_error.file_error.native_error == ENOSPC,
        "audit failure is attributed before the optional book write");

    const OutputSetWriteError repeated =
        output->write_batch(0U, batch);
    context.expect(
        repeated.code == OutputSetWriteErrorCode::file_failure &&
            repeated.file_error.native_error == ENOSPC,
        "first output failure is returned by every later write");
    static_cast<void>(output->close_all());
    context.expect(
        operations->output_for(book_descriptor) ==
            kOrderBookCsvHeader,
        "book row is never accepted after its audit row fails");
}

void test_close_best_effort_preserves_first_error(Context& context) {
    TemporaryDirectory parent;
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    auto operations = std::make_shared<TrackingFileOperations>();
    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_live(
        (parent.path() / "close").string(),
        PayloadVenue::spot,
        symbols.data(),
        symbols.size(),
        "2026-07-26",
        operations,
        open_error);
    if (!output) {
        context.expect(false, "close-failure output set opens");
        return;
    }
    const int audit_descriptor =
        operations->descriptor_for(output->audit_path(0U));
    const int book_descriptor =
        operations->descriptor_for(output->order_book_path(0U));
    operations->close_error_descriptors.insert(audit_descriptor);
    operations->close_error_descriptors.insert(book_descriptor);

    const OutputSetWriteError close_error = output->close_all();
    context.expect(
        close_error.code == OutputSetWriteErrorCode::file_failure &&
            close_error.file_kind == CsvFileKind::market_data &&
            close_error.file_error.code ==
                FileSinkErrorCode::close_failed &&
            operations->closed_descriptors.size() == 2U,
        "close checks every handle while preserving the first failure");
}

}  // namespace

void run_csv_output_set_tests(Context& context) {
    test_live_initialization_routing_and_exact_files(context);
    test_replay_book_only_contract(context);
    test_preflight_validation(context);
    test_maximum_live_target_capacity(context);
    test_initialization_rollback(context);
    test_rollback_failure_is_observable(context);
    test_audit_before_book_and_first_failure(context);
    test_close_best_effort_preserves_first_error(context);
}

}  // namespace hft::test
