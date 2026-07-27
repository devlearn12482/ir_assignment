#include "hft/csv_writer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <new>
#include <thread>
#include <utility>

namespace hft {
namespace {

constexpr std::size_t kSlotMask{kCsvWriterSlotCount - 1U};
constexpr std::size_t kPauseSlots{
    kCsvWriterSlotCount * 3U / 4U};
constexpr std::size_t kResumeSlots{
    kCsvWriterSlotCount / 2U};
constexpr std::uint64_t kPauseBytes{
    kCsvWriterByteCapacity * 3U / 4U};
constexpr std::uint64_t kResumeBytes{
    kCsvWriterByteCapacity / 2U};
constexpr auto kAgeFlushInterval = std::chrono::seconds{1};

static_assert(
    (kCsvWriterSlotCount & (kCsvWriterSlotCount - 1U)) == 0U);

[[nodiscard]] bool valid_batch(
    const EventRowBatch& batch,
    const CsvOutputMode mode) noexcept {
    if (!batch.has_audit_row &&
        !batch.has_order_book_row) {
        return false;
    }
    if (batch.has_audit_row == batch.audit_row.empty() ||
        batch.has_order_book_row == batch.order_book_row.empty()) {
        return false;
    }
    if (mode == CsvOutputMode::live_capture) {
        return !batch.has_order_book_row || batch.has_audit_row;
    }
    return !batch.has_audit_row;
}

[[nodiscard]] bool checked_add(
    const std::uint64_t left,
    const std::uint64_t right,
    std::uint64_t& result) noexcept {
    if (right >
        std::numeric_limits<std::uint64_t>::max() - left) {
        return false;
    }
    result = left + right;
    return true;
}

}  // namespace

struct CsvWriter::Impl {
    struct Slot {
        EventRowBatch batch{};
        std::size_t target_index{kNoSymbolIndex};
        std::uint64_t logical_bytes{0};
        std::uint64_t overflow_capacity_bytes{0};
    };

    std::unique_ptr<CsvOutputSet> output;
    std::unique_ptr<Slot[]> slots{
        std::make_unique<Slot[]>(kCsvWriterSlotCount)};
    std::thread thread{};
    std::mutex sleep_mutex{};
    std::condition_variable wake{};

    alignas(64) std::atomic<std::uint64_t> published_sequence{0};
    alignas(64) std::atomic<std::uint64_t> released_sequence{0};
    alignas(64) std::atomic<std::uint64_t> bytes_published{0};
    alignas(64) std::atomic<std::uint64_t> bytes_released{0};
    alignas(64) std::atomic<std::uint64_t>
        overflow_bytes_released{0};
    alignas(64) std::atomic<bool> close_requested{false};
    alignas(64) std::atomic<bool> writer_failed{false};
    alignas(64) std::atomic<bool> publish_active{false};
    alignas(64) std::atomic<bool> writer_sleeping{false};

    std::uint64_t producer_sequence{0};
    std::uint64_t producer_bytes{0};
    std::uint64_t producer_overflow_bytes{0};
    bool slot_acquired{false};
    std::uint64_t consumer_sequence{0};
    std::uint64_t consumer_bytes{0};
    std::uint64_t consumer_overflow_bytes{0};
    CsvWriterMetrics metrics{};
    CsvWriterResult final_result{};

    explicit Impl(std::unique_ptr<CsvOutputSet> configured_output)
        : output{std::move(configured_output)} {}

    [[nodiscard]] CsvWriterOccupancy occupancy() const noexcept {
        const std::uint64_t released =
            released_sequence.load(std::memory_order_acquire);
        const std::uint64_t released_bytes =
            bytes_released.load(std::memory_order_acquire);
        const std::uint64_t occupied_slots =
            producer_sequence >= released
                ? producer_sequence - released
                : 0U;
        const std::uint64_t occupied_bytes =
            producer_bytes >= released_bytes
                ? producer_bytes - released_bytes
                : 0U;
        const std::uint64_t released_overflow =
            overflow_bytes_released.load(
                std::memory_order_acquire);
        const std::uint64_t occupied_overflow =
            producer_overflow_bytes >= released_overflow
                ? producer_overflow_bytes - released_overflow
                : 0U;
        return CsvWriterOccupancy{
            static_cast<std::size_t>(occupied_slots),
            occupied_bytes,
            occupied_overflow};
    }

    void notify_writer() noexcept {
        if (!writer_sleeping.load(std::memory_order_acquire)) {
            return;
        }
        std::lock_guard<std::mutex> lock{sleep_mutex};
        wake.notify_one();
    }

    void release_slot(Slot& slot) noexcept {
        consumer_bytes += slot.logical_bytes;
        consumer_overflow_bytes +=
            slot.overflow_capacity_bytes;
        if (!slot.batch.release_excess_capacity()) {
            ++metrics.overflow_release_failures;
        }
        slot.target_index = kNoSymbolIndex;
        slot.logical_bytes = 0U;
        slot.overflow_capacity_bytes = 0U;
        ++consumer_sequence;
        bytes_released.store(
            consumer_bytes, std::memory_order_release);
        overflow_bytes_released.store(
            consumer_overflow_bytes,
            std::memory_order_release);
        released_sequence.store(
            consumer_sequence, std::memory_order_release);
    }

    void discard_published() noexcept {
        const std::uint64_t published =
            published_sequence.load(std::memory_order_acquire);
        while (consumer_sequence < published) {
            Slot& slot = slots[
                static_cast<std::size_t>(
                    consumer_sequence & kSlotMask)];
            release_slot(slot);
        }
    }

    void latch_output_failure(
        const OutputSetWriteError error) noexcept {
        if (!final_result.output_error) {
            final_result.output_error = error;
        }
        writer_failed.store(true, std::memory_order_release);
        while (publish_active.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void run() noexcept {
        auto next_flush =
            std::chrono::steady_clock::now() +
            kAgeFlushInterval;
        for (;;) {
            const std::uint64_t published =
                published_sequence.load(
                    std::memory_order_acquire);
            while (consumer_sequence < published &&
                   !writer_failed.load(
                       std::memory_order_acquire)) {
                Slot& slot = slots[
                    static_cast<std::size_t>(
                        consumer_sequence & kSlotMask)];
                const OutputSetWriteError error =
                    output->write_batch(
                        slot.target_index, slot.batch);
                release_slot(slot);
                if (error) {
                    latch_output_failure(error);
                }
            }

            if (writer_failed.load(std::memory_order_acquire)) {
                discard_published();
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= next_flush) {
                const CsvOutputSetMetrics before =
                    output->metrics();
                if (before.audit_rows_buffered != 0U ||
                    before.order_book_rows_buffered != 0U) {
                    const OutputSetWriteError error =
                        output->flush_all();
                    if (error) {
                        latch_output_failure(error);
                        discard_published();
                        break;
                    }
                    ++metrics.age_flushes;
                }
                next_flush = now + kAgeFlushInterval;
            }

            if (close_requested.load(
                    std::memory_order_acquire) &&
                consumer_sequence ==
                    published_sequence.load(
                        std::memory_order_acquire)) {
                break;
            }

            std::unique_lock<std::mutex> lock{sleep_mutex};
            writer_sleeping.store(true, std::memory_order_release);
            wake.wait_until(
                lock,
                next_flush,
                [this]() noexcept {
                    return close_requested.load(
                               std::memory_order_acquire) ||
                           writer_failed.load(
                               std::memory_order_acquire) ||
                           consumer_sequence !=
                               published_sequence.load(
                                   std::memory_order_acquire);
                });
            writer_sleeping.store(false, std::memory_order_release);
        }

        const OutputSetWriteError close_error =
            output->close_all();
        if (close_error) {
            latch_output_failure(close_error);
        }
        const CsvOutputSetMetrics output_metrics =
            output->metrics();
        metrics.audit_rows_written =
            output_metrics.audit_rows_written;
        metrics.order_book_rows_written =
            output_metrics.order_book_rows_written;
        metrics.audit_rows_unwritten =
            metrics.audit_rows_published >=
                    metrics.audit_rows_written
                ? metrics.audit_rows_published -
                      metrics.audit_rows_written
                : 0U;
        metrics.order_book_rows_unwritten =
            metrics.order_book_rows_published >=
                    metrics.order_book_rows_written
                ? metrics.order_book_rows_published -
                      metrics.order_book_rows_written
                : 0U;
        final_result.metrics = metrics;
    }
};

std::unique_ptr<CsvWriter> CsvWriter::start(
    std::unique_ptr<CsvOutputSet> output,
    CsvWriterCreateError& error) noexcept {
    error = CsvWriterCreateError::none;
    if (!output || output->closed() || output->failed() ||
        output->target_count() == 0U) {
        error = CsvWriterCreateError::invalid_output;
        return nullptr;
    }
    try {
        std::unique_ptr<CsvWriter> writer{
            new CsvWriter{std::move(output)}};
        try {
            writer->impl_->thread = std::thread{
                [impl = writer->impl_.get()]() noexcept {
                    impl->run();
                }};
        } catch (...) {
            static_cast<void>(writer->impl_->output->close_all());
            error = CsvWriterCreateError::thread_start_failure;
            return nullptr;
        }
        return writer;
    } catch (const std::bad_alloc&) {
        error = CsvWriterCreateError::allocation_failure;
        return nullptr;
    } catch (...) {
        error = CsvWriterCreateError::allocation_failure;
        return nullptr;
    }
}

CsvWriter::CsvWriter(std::unique_ptr<CsvOutputSet> output)
    : impl_{std::make_unique<Impl>(std::move(output))} {}

CsvWriter::~CsvWriter() noexcept {
    if (impl_) {
        impl_->close_requested.store(
            true, std::memory_order_release);
        impl_->wake.notify_one();
        if (impl_->thread.joinable()) {
            impl_->thread.join();
        }
    }
}

EventRowBatch* CsvWriter::try_acquire(
    CsvWriterAcquireError& error) noexcept {
    error = CsvWriterAcquireError::none;
    if (!impl_) {
        error = CsvWriterAcquireError::failed;
        return nullptr;
    }
    if (impl_->slot_acquired) {
        error = CsvWriterAcquireError::already_acquired;
        return nullptr;
    }
    if (impl_->close_requested.load(std::memory_order_acquire)) {
        error = CsvWriterAcquireError::closed;
        return nullptr;
    }
    if (impl_->writer_failed.load(std::memory_order_acquire)) {
        error = CsvWriterAcquireError::failed;
        return nullptr;
    }
    const std::uint64_t released =
        impl_->released_sequence.load(std::memory_order_acquire);
    if (impl_->producer_sequence - released >=
        kCsvWriterSlotCount) {
        error = CsvWriterAcquireError::slot_capacity;
        return nullptr;
    }
    Impl::Slot& slot = impl_->slots[
        static_cast<std::size_t>(
            impl_->producer_sequence & kSlotMask)];
    slot.batch.clear();
    slot.target_index = kNoSymbolIndex;
    slot.logical_bytes = 0U;
    slot.overflow_capacity_bytes = 0U;
    impl_->slot_acquired = true;
    return &slot.batch;
}

CsvWriterPublishError CsvWriter::publish(
    const std::size_t target_index) noexcept {
    if (!impl_ || !impl_->slot_acquired) {
        return CsvWriterPublishError::no_acquired_slot;
    }
    Impl::Slot& slot = impl_->slots[
        static_cast<std::size_t>(
            impl_->producer_sequence & kSlotMask)];
    impl_->publish_active.store(true, std::memory_order_release);
    const auto reject =
        [this, &slot](const CsvWriterPublishError error) noexcept {
            static_cast<void>(
                slot.batch.release_excess_capacity());
            slot.target_index = kNoSymbolIndex;
            slot.logical_bytes = 0U;
            slot.overflow_capacity_bytes = 0U;
            impl_->slot_acquired = false;
            impl_->publish_active.store(
                false, std::memory_order_release);
            return error;
        };
    if (impl_->close_requested.load(std::memory_order_acquire)) {
        return reject(CsvWriterPublishError::closed);
    }
    if (impl_->writer_failed.load(std::memory_order_acquire)) {
        return reject(CsvWriterPublishError::failed);
    }
    if (target_index >= impl_->output->target_count()) {
        return reject(CsvWriterPublishError::invalid_target);
    }
    if (!valid_batch(slot.batch, impl_->output->mode())) {
        return reject(CsvWriterPublishError::invalid_batch);
    }
    const std::uint64_t logical_bytes =
        static_cast<std::uint64_t>(slot.batch.audit_row.size()) +
        static_cast<std::uint64_t>(
            slot.batch.order_book_row.size());
    const std::uint64_t released_bytes =
        impl_->bytes_released.load(std::memory_order_acquire);
    const std::uint64_t occupied_bytes =
        impl_->producer_bytes - released_bytes;
    if (logical_bytes > kCsvWriterByteCapacity ||
        occupied_bytes >
            kCsvWriterByteCapacity - logical_bytes) {
        return reject(CsvWriterPublishError::byte_capacity);
    }
    std::uint64_t next_sequence = 0;
    std::uint64_t next_bytes = 0;
    if (!checked_add(
            impl_->producer_sequence, 1U, next_sequence) ||
        !checked_add(
            impl_->producer_bytes,
            logical_bytes,
            next_bytes)) {
        return reject(CsvWriterPublishError::counter_overflow);
    }

    slot.target_index = target_index;
    slot.logical_bytes = logical_bytes;
    const std::size_t audit_excess =
        slot.batch.audit_row.capacity() >
                slot.batch.audit_row.initial_capacity()
            ? slot.batch.audit_row.capacity() -
                  slot.batch.audit_row.initial_capacity()
            : 0U;
    const std::size_t book_excess =
        slot.batch.order_book_row.capacity() >
                slot.batch.order_book_row.initial_capacity()
            ? slot.batch.order_book_row.capacity() -
                  slot.batch.order_book_row.initial_capacity()
            : 0U;
    const std::uint64_t overflow_increment =
        static_cast<std::uint64_t>(audit_excess) +
        static_cast<std::uint64_t>(book_excess);
    std::uint64_t next_overflow_bytes = 0;
    if (!checked_add(
            impl_->metrics.overflow_capacity_bytes,
            overflow_increment,
            next_overflow_bytes)) {
        return reject(CsvWriterPublishError::counter_overflow);
    }
    std::uint64_t next_active_overflow_bytes = 0;
    if (!checked_add(
            impl_->producer_overflow_bytes,
            overflow_increment,
            next_active_overflow_bytes)) {
        return reject(CsvWriterPublishError::counter_overflow);
    }
    slot.overflow_capacity_bytes = overflow_increment;
    impl_->producer_sequence = next_sequence;
    impl_->producer_bytes = next_bytes;
    impl_->producer_overflow_bytes =
        next_active_overflow_bytes;
    impl_->slot_acquired = false;
    ++impl_->metrics.batches_published;
    impl_->metrics.logical_bytes_published += logical_bytes;
    impl_->metrics.audit_rows_published +=
        slot.batch.has_audit_row ? 1U : 0U;
    impl_->metrics.order_book_rows_published +=
        slot.batch.has_order_book_row ? 1U : 0U;
    if (audit_excess != 0U || book_excess != 0U) {
        ++impl_->metrics.overflow_batches_published;
        impl_->metrics.overflow_capacity_bytes =
            next_overflow_bytes;
    }
    const CsvWriterOccupancy current = impl_->occupancy();
    impl_->metrics.slot_high_water = std::max(
        impl_->metrics.slot_high_water, current.slots);
    impl_->metrics.byte_high_water = std::max(
        impl_->metrics.byte_high_water,
        current.logical_bytes);
    impl_->metrics.overflow_capacity_high_water = std::max(
        impl_->metrics.overflow_capacity_high_water,
        current.overflow_capacity_bytes);
    impl_->bytes_published.store(
        impl_->producer_bytes, std::memory_order_release);
    impl_->published_sequence.store(
        impl_->producer_sequence, std::memory_order_release);
    impl_->publish_active.store(false, std::memory_order_release);
    impl_->notify_writer();
    return CsvWriterPublishError::none;
}

void CsvWriter::cancel() noexcept {
    if (!impl_ || !impl_->slot_acquired) {
        return;
    }
    Impl::Slot& slot = impl_->slots[
        static_cast<std::size_t>(
            impl_->producer_sequence & kSlotMask)];
    static_cast<void>(slot.batch.release_excess_capacity());
    slot.target_index = kNoSymbolIndex;
    slot.logical_bytes = 0U;
    slot.overflow_capacity_bytes = 0U;
    impl_->slot_acquired = false;
}

CsvWriterOccupancy CsvWriter::occupancy() const noexcept {
    return impl_ ? impl_->occupancy() : CsvWriterOccupancy{};
}

bool CsvWriter::should_pause() const noexcept {
    const CsvWriterOccupancy current = occupancy();
    return current.slots >= kPauseSlots ||
           current.logical_bytes >= kPauseBytes;
}

bool CsvWriter::below_resume_watermark() const noexcept {
    const CsvWriterOccupancy current = occupancy();
    return current.slots < kResumeSlots &&
           current.logical_bytes < kResumeBytes;
}

bool CsvWriter::failed() const noexcept {
    return !impl_ ||
           impl_->writer_failed.load(std::memory_order_acquire);
}

void CsvWriter::close() noexcept {
    if (!impl_) {
        return;
    }
    cancel();
    impl_->close_requested.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock{impl_->sleep_mutex};
    impl_->wake.notify_one();
}

CsvWriterResult CsvWriter::join() noexcept {
    if (!impl_) {
        CsvWriterResult result;
        result.output_error.code =
            OutputSetWriteErrorCode::closed;
        return result;
    }
    close();
    if (impl_->thread.joinable()) {
        impl_->thread.join();
    }
    return impl_->final_result;
}

}  // namespace hft
