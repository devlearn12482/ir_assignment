#include "test_framework.h"

#include "hft/csv_writer.h"

#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
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
            ("hft_csv_writer_" + std::to_string(::getpid()) + "_" +
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

class WriterFileOperations final : public FileOperations {
public:
    [[nodiscard]] FileOpenResult open_exclusive(
        const std::string& path) noexcept override {
        std::lock_guard<std::mutex> lock{mutex_};
        const int descriptor = next_descriptor_++;
        path_by_descriptor_[descriptor] = path;
        output_by_descriptor_[descriptor] = {};
        return FileOpenResult{descriptor, 0};
    }

    [[nodiscard]] FileWriteResult write(
        const int descriptor,
        const char* const data,
        const std::size_t size) noexcept override {
        std::unique_lock<std::mutex> lock{mutex_};
        if (blocking_enabled_) {
            write_blocked_ = true;
            blocked_.notify_all();
            released_.wait(
                lock, [this]() noexcept { return release_writes_; });
        }
        if (descriptor == fail_descriptor_ && fail_next_write_) {
            fail_next_write_ = false;
            return FileWriteResult{-1, ENOSPC};
        }
        output_by_descriptor_[descriptor].append(data, size);
        written_.notify_all();
        return FileWriteResult{
            static_cast<std::ptrdiff_t>(size), 0};
    }

    [[nodiscard]] FileCloseResult close(
        const int) noexcept override {
        return FileCloseResult{true, 0};
    }

    [[nodiscard]] FileRemoveResult remove(
        const std::string&) noexcept override {
        return FileRemoveResult{true, 0};
    }

    [[nodiscard]] int descriptor_for(
        const std::string_view path) const {
        std::lock_guard<std::mutex> lock{mutex_};
        for (const auto& item : path_by_descriptor_) {
            if (item.second == path) {
                return item.first;
            }
        }
        return -1;
    }

    [[nodiscard]] std::string output_for(
        const int descriptor) const {
        std::lock_guard<std::mutex> lock{mutex_};
        return output_by_descriptor_.at(descriptor);
    }

    [[nodiscard]] bool wait_for_output_size(
        const int descriptor,
        const std::size_t minimum_size) {
        std::unique_lock<std::mutex> lock{mutex_};
        return written_.wait_for(
            lock,
            std::chrono::seconds{2},
            [this, descriptor, minimum_size]() {
                return output_by_descriptor_.at(descriptor).size() >=
                       minimum_size;
            });
    }

    void fail_next_write(const int descriptor) noexcept {
        std::lock_guard<std::mutex> lock{mutex_};
        fail_descriptor_ = descriptor;
        fail_next_write_ = true;
    }

    void enable_blocking() noexcept {
        std::lock_guard<std::mutex> lock{mutex_};
        blocking_enabled_ = true;
        write_blocked_ = false;
        release_writes_ = false;
    }

    [[nodiscard]] bool wait_until_blocked() {
        std::unique_lock<std::mutex> lock{mutex_};
        return blocked_.wait_for(
            lock,
            std::chrono::seconds{3},
            [this]() noexcept { return write_blocked_; });
    }

    void release_writes() noexcept {
        std::lock_guard<std::mutex> lock{mutex_};
        blocking_enabled_ = false;
        release_writes_ = true;
        released_.notify_all();
    }

private:
    mutable std::mutex mutex_{};
    std::condition_variable blocked_{};
    std::condition_variable released_{};
    std::condition_variable written_{};
    int next_descriptor_{100};
    int fail_descriptor_{-1};
    bool fail_next_write_{false};
    bool blocking_enabled_{false};
    bool write_blocked_{false};
    bool release_writes_{false};
    std::map<int, std::string> path_by_descriptor_{};
    std::map<int, std::string> output_by_descriptor_{};
};

[[nodiscard]] std::unique_ptr<CsvOutputSet> open_output(
    Context& context,
    const TemporaryDirectory& parent,
    const std::shared_ptr<FileOperations>& operations,
    std::string& audit_path,
    std::string& book_path) {
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    OutputSetOpenError error;
    std::unique_ptr<CsvOutputSet> output =
        CsvOutputSet::open_live(
            (parent.path() / "capture").string(),
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            "2026-07-27",
            operations,
            error);
    context.expect(
        output != nullptr && !error,
        "writer test output opens");
    if (output) {
        audit_path = std::string{output->audit_path(0U)};
        book_path = std::string{output->order_book_path(0U)};
    }
    return output;
}

void fill_audit(
    EventRowBatch& batch,
    const std::uint64_t connection_sequence,
    const std::string_view payload = "{}") {
    batch.clear();
    const MarketDataCsvRow row{
        CsvTimestamp{1'700'000'000U, 123U},
        PayloadVenue::spot,
        SpotStreamKind::trade,
        0U,
        0U,
        connection_sequence,
        "BTCUSDT",
        payload,
    };
    if (format_market_data_csv_row(row, batch.audit_row) ==
        CsvFormatError::none) {
        batch.has_audit_row = true;
    }
}

void fill_book(EventRowBatch& batch) {
    constexpr std::array<BookLevel, 1U> bids{{
        BookLevel{10'000'000'000LL, 100'000'000LL},
    }};
    constexpr std::array<BookLevel, 1U> asks{{
        BookLevel{10'100'000'000LL, 200'000'000LL},
    }};
    const OrderBookCsvRow row{
        CsvTimestamp{1'700'000'000U, 123U},
        1U,
        123,
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

[[nodiscard]] std::unique_ptr<CsvWriter> start_writer(
    Context& context,
    std::unique_ptr<CsvOutputSet> output) {
    CsvWriterCreateError error;
    std::unique_ptr<CsvWriter> writer =
        CsvWriter::start(std::move(output), error);
    context.expect(
        writer != nullptr &&
            error == CsvWriterCreateError::none,
        "dedicated CSV writer starts");
    return writer;
}

void test_ordered_drain_and_metrics(Context& context) {
    TemporaryDirectory parent;
    auto operations = std::make_shared<WriterFileOperations>();
    std::string audit_path;
    std::string book_path;
    std::unique_ptr<CsvOutputSet> output = open_output(
        context, parent, operations, audit_path, book_path);
    if (!output) {
        return;
    }
    const int audit_descriptor =
        operations->descriptor_for(audit_path);
    const int book_descriptor =
        operations->descriptor_for(book_path);
    std::unique_ptr<CsvWriter> writer =
        start_writer(context, std::move(output));
    if (!writer) {
        return;
    }

    CsvWriterAcquireError acquire_error;
    EventRowBatch* first = writer->try_acquire(acquire_error);
    if (!first) {
        context.expect(false, "producer acquires first writer slot");
        return;
    }
    fill_audit(*first, 1U);
    fill_book(*first);
    const std::string first_audit{first->audit_row.view()};
    const std::string first_book{first->order_book_row.view()};
    context.expect(
        writer->publish(0U) == CsvWriterPublishError::none,
        "audit-plus-book batch publishes atomically");

    EventRowBatch* second = writer->try_acquire(acquire_error);
    if (second) {
        fill_audit(*second, 2U);
    }
    const std::string second_audit{
        second ? second->audit_row.view() : std::string_view{}};
    context.expect(
        second != nullptr &&
            writer->publish(0U) ==
                CsvWriterPublishError::none,
        "audit-only batch follows in producer order");

    const CsvWriterResult result = writer->join();
    context.expect(
        result.success() &&
            result.metrics.batches_published == 2U &&
            result.metrics.audit_rows_published == 2U &&
            result.metrics.audit_rows_written == 2U &&
            result.metrics.audit_rows_unwritten == 0U &&
            result.metrics.order_book_rows_published == 1U &&
            result.metrics.order_book_rows_written == 1U &&
            result.metrics.order_book_rows_unwritten == 0U,
        "checked drain reports exact row accounting");
    context.expect(
        operations->output_for(audit_descriptor) ==
                std::string{kMarketDataCsvHeader} +
                    first_audit + second_audit &&
            operations->output_for(book_descriptor) ==
                std::string{kOrderBookCsvHeader} + first_book,
        "writer preserves batch and file ordering exactly");
}

void test_producer_contract(Context& context) {
    TemporaryDirectory parent;
    auto operations = std::make_shared<WriterFileOperations>();
    std::string audit_path;
    std::string book_path;
    std::unique_ptr<CsvOutputSet> output = open_output(
        context, parent, operations, audit_path, book_path);
    if (!output) {
        return;
    }
    std::unique_ptr<CsvWriter> writer =
        start_writer(context, std::move(output));
    if (!writer) {
        return;
    }

    CsvWriterAcquireError error;
    EventRowBatch* batch = writer->try_acquire(error);
    CsvWriterAcquireError duplicate_error;
    context.expect(
        batch != nullptr &&
            writer->try_acquire(duplicate_error) == nullptr &&
            duplicate_error ==
                CsvWriterAcquireError::already_acquired,
        "one producer cannot hold two ring slots");
    writer->cancel();

    batch = writer->try_acquire(error);
    context.expect(
        batch != nullptr &&
            writer->publish(0U) ==
                CsvWriterPublishError::invalid_batch,
        "empty batches are rejected and the slot is released");
    batch = writer->try_acquire(error);
    if (batch) {
        fill_audit(*batch, 1U);
    }
    context.expect(
        batch != nullptr &&
            writer->publish(1U) ==
                CsvWriterPublishError::invalid_target,
        "invalid targets fail before publication");

    writer->close();
    context.expect(
        writer->try_acquire(error) == nullptr &&
            error == CsvWriterAcquireError::closed &&
            writer->join().success(),
        "closed producer rejects work and an empty drain succeeds");
}

void test_write_failure_accounts_unwritten_rows(Context& context) {
    TemporaryDirectory parent;
    auto operations = std::make_shared<WriterFileOperations>();
    std::string audit_path;
    std::string book_path;
    std::unique_ptr<CsvOutputSet> output = open_output(
        context, parent, operations, audit_path, book_path);
    if (!output) {
        return;
    }
    const int audit_descriptor =
        operations->descriptor_for(audit_path);
    operations->fail_next_write(audit_descriptor);
    std::unique_ptr<CsvWriter> writer =
        start_writer(context, std::move(output));
    if (!writer) {
        return;
    }

    for (std::uint64_t sequence = 1U; sequence <= 3U;
         ++sequence) {
        CsvWriterAcquireError error;
        EventRowBatch* batch = writer->try_acquire(error);
        if (!batch) {
            context.expect(false, "failure test acquires all slots");
            break;
        }
        fill_audit(*batch, sequence);
        context.expect(
            writer->publish(0U) ==
                CsvWriterPublishError::none,
            "row publishes before injected drain failure");
    }

    const CsvWriterResult result = writer->join();
    context.expect(
        !result.success() && result.output_error &&
            result.output_error.code ==
                OutputSetWriteErrorCode::file_failure &&
            result.metrics.audit_rows_published == 3U &&
            result.metrics.audit_rows_written == 0U &&
            result.metrics.audit_rows_unwritten == 3U &&
            writer->failed(),
        "shutdown write failure is visible and forces nonzero semantics");
}

void test_one_second_age_flush(Context& context) {
    TemporaryDirectory parent;
    auto operations = std::make_shared<WriterFileOperations>();
    std::string audit_path;
    std::string book_path;
    std::unique_ptr<CsvOutputSet> output = open_output(
        context, parent, operations, audit_path, book_path);
    if (!output) {
        return;
    }
    const int audit_descriptor =
        operations->descriptor_for(audit_path);
    std::unique_ptr<CsvWriter> writer =
        start_writer(context, std::move(output));
    if (!writer) {
        return;
    }

    CsvWriterAcquireError error;
    EventRowBatch* batch = writer->try_acquire(error);
    if (batch) {
        fill_audit(*batch, 1U);
    }
    context.expect(
        batch != nullptr &&
            writer->publish(0U) ==
                CsvWriterPublishError::none &&
            operations->wait_for_output_size(
                audit_descriptor,
                kMarketDataCsvHeader.size() + 1U),
        "low-volume row is flushed by the one-second age deadline");
    const CsvWriterResult result = writer->join();
    context.expect(
        result.success() &&
            result.metrics.age_flushes == 1U,
        "age-triggered flush is counted exactly");
}

void test_slot_capacity_and_watermarks(Context& context) {
    TemporaryDirectory parent;
    auto operations = std::make_shared<WriterFileOperations>();
    std::string audit_path;
    std::string book_path;
    std::unique_ptr<CsvOutputSet> output = open_output(
        context, parent, operations, audit_path, book_path);
    if (!output) {
        return;
    }
    std::unique_ptr<CsvWriter> writer =
        start_writer(context, std::move(output));
    if (!writer) {
        return;
    }

    operations->enable_blocking();
    std::string large_payload(
        kCsvAggregationBufferBytes + 1U, 'x');
    CsvWriterAcquireError error;
    EventRowBatch* first = writer->try_acquire(error);
    if (first) {
        fill_audit(*first, 1U, large_payload);
    }
    context.expect(
        first != nullptr &&
            writer->publish(0U) ==
                CsvWriterPublishError::none &&
            operations->wait_until_blocked(),
        "large first row deterministically blocks the consumer");

    bool all_published = true;
    for (std::size_t index = 1U;
         index < kCsvWriterSlotCount;
         ++index) {
        EventRowBatch* batch = writer->try_acquire(error);
        if (!batch) {
            all_published = false;
            break;
        }
        fill_audit(
            *batch, static_cast<std::uint64_t>(index + 1U));
        if (writer->publish(0U) !=
            CsvWriterPublishError::none) {
            all_published = false;
            break;
        }
    }
    const CsvWriterOccupancy full = writer->occupancy();
    EventRowBatch* overflow = writer->try_acquire(error);
    context.expect(
        all_published && full.slots == kCsvWriterSlotCount &&
            writer->should_pause() &&
            overflow == nullptr &&
            error == CsvWriterAcquireError::slot_capacity,
        "hard slot bound and high-water pause are deterministic");

    operations->release_writes();
    const CsvWriterResult result = writer->join();
    context.expect(
        result.success() &&
            result.metrics.batches_published ==
                kCsvWriterSlotCount &&
            result.metrics.slot_high_water ==
                kCsvWriterSlotCount &&
            result.metrics.overflow_batches_published == 1U &&
            result.metrics.overflow_capacity_bytes != 0U &&
            result.metrics.overflow_capacity_high_water != 0U &&
            result.metrics.overflow_release_failures == 0U &&
            writer->below_resume_watermark(),
        "drain releases all slots below the resume watermark");
}

}  // namespace

void run_csv_writer_tests(Context& context) {
    test_ordered_drain_and_metrics(context);
    test_producer_contract(context);
    test_write_failure_accounts_unwritten_rows(context);
    test_one_second_age_flush(context);
    test_slot_capacity_and_watermarks(context);
}

}  // namespace hft::test
