#include "test_framework.h"

#include "hft/buffered_file_sink.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
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
        path_ = std::filesystem::temp_directory_path() /
            ("hft_file_sink_" + std::to_string(::getpid()));
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
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
    std::filesystem::path path_;
};

struct WriteAction {
    std::ptrdiff_t bytes{0};
    int native_error{0};
};

class FakeFileOperations final : public FileOperations {
public:
    [[nodiscard]] FileOpenResult open_exclusive(
        const std::string& path) noexcept override {
        ++open_calls;
        last_path = path;
        if (open_error != 0) {
            return FileOpenResult{-1, open_error};
        }
        return FileOpenResult{descriptor, 0};
    }

    [[nodiscard]] FileWriteResult write(
        const int,
        const char* const data,
        const std::size_t size) noexcept override {
        ++write_calls;
        if (next_action < actions.size()) {
            const WriteAction action = actions[next_action++];
            if (action.bytes < 0) {
                return FileWriteResult{-1, action.native_error};
            }
            const auto accepted = std::min(
                static_cast<std::size_t>(action.bytes), size);
            output.append(data, accepted);
            return FileWriteResult{
                static_cast<std::ptrdiff_t>(accepted), 0};
        }
        output.append(data, size);
        return FileWriteResult{
            static_cast<std::ptrdiff_t>(size), 0};
    }

    [[nodiscard]] FileCloseResult close(
        const int) noexcept override {
        ++close_calls;
        if (close_error != 0) {
            return FileCloseResult{false, close_error};
        }
        return FileCloseResult{true, 0};
    }

    [[nodiscard]] FileRemoveResult remove(
        const std::string& path) noexcept override {
        ++remove_calls;
        removed_path = path;
        return FileRemoveResult{
            remove_succeeds, remove_succeeds ? 0 : EIO};
    }

    void reset_write_observation() {
        actions.clear();
        next_action = 0U;
        output.clear();
        write_calls = 0U;
    }

    int descriptor{17};
    int open_error{0};
    int close_error{0};
    bool remove_succeeds{true};
    std::vector<WriteAction> actions;
    std::size_t next_action{0};
    std::string output;
    std::string last_path;
    std::string removed_path;
    std::size_t open_calls{0};
    std::size_t write_calls{0};
    std::size_t close_calls{0};
    std::size_t remove_calls{0};
};

[[nodiscard]] std::string read_binary_file(
    const std::filesystem::path& path) {
    std::ifstream input{path, std::ios::binary};
    return std::string{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}};
}

void test_real_file_exact_bytes_and_exclusive_create(Context& context) {
    TemporaryDirectory directory;
    const auto path = directory.path() / "market_data.csv";

    FileSinkError error;
    auto sink = BufferedCsvFileSink::open_exclusive(
        path.string(), CsvFileKind::market_data, error);
    context.expect(
        sink != nullptr && !error,
        "real market-data sink opens exclusively");
    if (!sink) {
        return;
    }

    context.expect(
        !sink->append_record("1,2,test\n"),
        "valid record is accepted");
    context.expect(
        sink->buffered_bytes() == 9U &&
            sink->buffered_rows() == 1U,
        "ordinary record remains buffered before close");
    context.expect(
        !sink->close() && !sink->is_open(),
        "checked close flushes and closes the file");

    context.expect(
        read_binary_file(path) ==
            std::string{kMarketDataCsvHeader} + "1,2,test\n",
        "real file has exact header, LF, and data bytes");

    FileSinkError duplicate_error;
    auto duplicate = BufferedCsvFileSink::open_exclusive(
        path.string(), CsvFileKind::order_book, duplicate_error);
    context.expect(
        !duplicate &&
            duplicate_error.code == FileSinkErrorCode::open_failed &&
            duplicate_error.native_error == EEXIST,
        "exclusive create rejects an existing path without truncation");
    context.expect(
        read_binary_file(path) ==
            std::string{kMarketDataCsvHeader} + "1,2,test\n",
        "failed duplicate open leaves original file unchanged");
}

void test_buffering_and_direct_large_record(Context& context) {
    auto operations = std::make_shared<FakeFileOperations>();
    FileSinkError error;
    auto sink = BufferedCsvFileSink::open_exclusive(
        "book.csv",
        CsvFileKind::order_book,
        operations,
        error);
    context.expect(
        sink != nullptr && !error &&
            operations->output == kOrderBookCsvHeader,
        "sink chooses and writes the exact internal book header");
    if (!sink) {
        return;
    }
    operations->reset_write_observation();

    context.expect(
        !sink->append_record("row-one\n") &&
            !sink->append_record("row-two\n") &&
            operations->write_calls == 0U,
        "ordinary rows aggregate without a native write");
    context.expect(
        !sink->flush() &&
            operations->output == "row-one\nrow-two\n",
        "explicit flush emits aggregated rows in order");
    context.expect(
        sink->metrics().rows_written == 2U &&
            sink->metrics().buffer_flushes == 1U,
        "successful aggregate flush accounts rows exactly");

    operations->reset_write_observation();
    context.expect(
        !sink->append_record("pending\n"),
        "small row buffers before a large row");
    std::string large(kCsvAggregationBufferBytes + 1U, 'x');
    large.back() = '\n';
    context.expect(
        !sink->append_record(large),
        "large row is accepted without truncation");
    context.expect(
        operations->output == std::string{"pending\n"} + large &&
            sink->buffered_bytes() == 0U,
        "large row drains prior bytes and writes directly");
    context.expect(
        sink->metrics().rows_written == 4U &&
            sink->metrics().buffer_flushes == 2U &&
            sink->metrics().direct_large_records == 1U,
        "direct-row and flush metrics remain exact");
    context.expect(!sink->close(), "fake sink closes successfully");
}

void test_partial_write_and_eintr_retry(Context& context) {
    auto operations = std::make_shared<FakeFileOperations>();
    FileSinkError error;
    auto sink = BufferedCsvFileSink::open_exclusive(
        "audit.csv",
        CsvFileKind::market_data,
        operations,
        error);
    if (!sink) {
        context.expect(false, "retry test sink opens");
        return;
    }
    operations->reset_write_observation();
    operations->actions = {
        WriteAction{-1, EINTR},
        WriteAction{2, 0},
        WriteAction{3, 0},
    };

    context.expect(
        !sink->append_record("abcd\n") && !sink->flush(),
        "EINTR and short writes are retried to completion");
    context.expect(
        operations->output == "abcd\n" &&
            operations->write_calls == 3U &&
            sink->metrics().native_write_calls == 4U,
        "write-all preserves bytes and counts every native attempt");
    context.expect(!sink->close(), "retry test closes successfully");
}

void test_first_write_failure_is_latched(Context& context) {
    auto operations = std::make_shared<FakeFileOperations>();
    FileSinkError error;
    auto sink = BufferedCsvFileSink::open_exclusive(
        "audit.csv",
        CsvFileKind::market_data,
        operations,
        error);
    if (!sink) {
        context.expect(false, "failure-latch test sink opens");
        return;
    }
    operations->reset_write_observation();
    operations->actions = {WriteAction{-1, ENOSPC}};

    context.expect(
        !sink->append_record("unwritten\n"),
        "row buffers before injected write failure");
    const FileSinkError flush_error = sink->flush();
    context.expect(
        flush_error.code == FileSinkErrorCode::write_failed &&
            flush_error.native_error == ENOSPC &&
            sink->failed() &&
            sink->buffered_rows() == 1U,
        "write failure is latched with buffered row accounting intact");

    const std::size_t calls_after_failure = operations->write_calls;
    const FileSinkError later_error =
        sink->append_record("later\n");
    context.expect(
        later_error.code == FileSinkErrorCode::write_failed &&
            later_error.native_error == ENOSPC &&
            operations->write_calls == calls_after_failure,
        "normal writes stop and return the preserved first error");
    const FileSinkError close_error = sink->close();
    context.expect(
        close_error.code == FileSinkErrorCode::write_failed &&
            close_error.native_error == ENOSPC &&
            operations->close_calls == 1U,
        "close still runs but cannot replace the first write error");
}

void test_open_and_close_failures(Context& context) {
    {
        auto operations = std::make_shared<FakeFileOperations>();
        operations->open_error = EACCES;
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "denied.csv",
            CsvFileKind::market_data,
            operations,
            error);
        context.expect(
            !sink &&
                error.code == FileSinkErrorCode::open_failed &&
                error.native_error == EACCES &&
                operations->close_calls == 0U,
            "open failure reports its native error without cleanup");
    }

    {
        auto operations = std::make_shared<FakeFileOperations>();
        operations->actions = {WriteAction{-1, EIO}};
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "header-failure.csv",
            CsvFileKind::market_data,
            operations,
            error);
        context.expect(
            !sink &&
                error.code == FileSinkErrorCode::write_failed &&
                error.native_error == EIO &&
                operations->close_calls == 1U &&
                operations->remove_calls == 1U &&
                operations->removed_path == "header-failure.csv",
            "header failure closes and removes the partial file");
    }

    {
        auto operations = std::make_shared<FakeFileOperations>();
        operations->actions = {WriteAction{-1, EIO}};
        operations->remove_succeeds = false;
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "header-cleanup-failure.csv",
            CsvFileKind::market_data,
            operations,
            error);
        context.expect(
            !sink &&
                error.code == FileSinkErrorCode::write_failed &&
                error.native_error == EIO &&
                error.cleanup_operation ==
                    FileSinkCleanupOperation::remove &&
                error.cleanup_native_error == EIO,
            "failed-open cleanup is observable without hiding the header error");
    }

    {
        auto operations = std::make_shared<FakeFileOperations>();
        operations->close_error = EIO;
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "close-failure.csv",
            CsvFileKind::order_book,
            operations,
            error);
        if (!sink) {
            context.expect(false, "close-failure test sink opens");
            return;
        }
        const FileSinkError close_error = sink->close();
        context.expect(
            close_error.code == FileSinkErrorCode::close_failed &&
                close_error.native_error == EIO &&
                !sink->is_open(),
            "checked close reports failure and never retries descriptor");
    }
}

void test_record_contract(Context& context) {
    {
        auto operations = std::make_shared<FakeFileOperations>();
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "record.csv",
            CsvFileKind::market_data,
            operations,
            error);
        if (!sink) {
            context.expect(false, "record-contract test sink opens");
            return;
        }
        operations->reset_write_observation();

        const FileSinkError invalid =
            sink->append_record("missing-lf");
        context.expect(
            invalid.code == FileSinkErrorCode::invalid_record &&
                operations->write_calls == 0U,
            "incomplete record is rejected before file I/O");
        context.expect(
            sink->append_record("valid\n").code ==
                FileSinkErrorCode::invalid_record,
            "record-contract failure is fatal and remains latched");
        static_cast<void>(sink->close());
    }

    {
        auto operations = std::make_shared<FakeFileOperations>();
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "oversized.csv",
            CsvFileKind::market_data,
            operations,
            error);
        if (!sink) {
            context.expect(false, "oversized-record test sink opens");
            return;
        }
        operations->reset_write_observation();
        std::string oversized(kMaxCsvRecordBytes + 1U, 'x');
        oversized.back() = '\n';
        context.expect(
            sink->append_record(oversized).code ==
                    FileSinkErrorCode::invalid_record &&
                operations->write_calls == 0U,
            "record above the hard limit is rejected without truncation");
        static_cast<void>(sink->close());
    }

    {
        auto operations = std::make_shared<FakeFileOperations>();
        FileSinkError error;
        auto sink = BufferedCsvFileSink::open_exclusive(
            "invalid-kind.csv",
            static_cast<CsvFileKind>(255U),
            operations,
            error);
        context.expect(
            !sink &&
                error.code == FileSinkErrorCode::invalid_file_kind &&
                operations->open_calls == 0U,
            "invalid file kind cannot create a headerless file");
    }
}

}  // namespace

void run_buffered_file_sink_tests(Context& context) {
    test_real_file_exact_bytes_and_exclusive_create(context);
    test_buffering_and_direct_large_record(context);
    test_partial_write_and_eintr_retry(context);
    test_first_write_failure_is_latched(context);
    test_open_and_close_failures(context);
    test_record_contract(context);
}

}  // namespace hft::test
