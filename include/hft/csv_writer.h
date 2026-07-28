#pragma once

#include "hft/csv_output_set.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace hft {

inline constexpr std::size_t kCsvWriterSlotCount{4096U};
inline constexpr std::uint64_t kCsvWriterByteCapacity{
    64U * 1024U * 1024U};

enum class CsvWriterCreateError : std::uint8_t {
    none,
    invalid_output,
    allocation_failure,
    thread_start_failure,
};

enum class CsvWriterAcquireError : std::uint8_t {
    none,
    closed,
    failed,
    already_acquired,
    slot_capacity,
};

enum class CsvWriterPublishError : std::uint8_t {
    none,
    no_acquired_slot,
    closed,
    failed,
    invalid_target,
    invalid_batch,
    byte_capacity,
    counter_overflow,
};

struct CsvWriterOccupancy {
    std::size_t slots{0};
    std::uint64_t logical_bytes{0};
    std::uint64_t overflow_capacity_bytes{0};
};

struct CsvWriterMetrics {
    std::uint64_t batches_published{0};
    std::uint64_t logical_bytes_published{0};
    std::uint64_t audit_rows_published{0};
    std::uint64_t order_book_rows_published{0};
    std::uint64_t audit_rows_written{0};
    std::uint64_t order_book_rows_written{0};
    std::uint64_t audit_rows_unwritten{0};
    std::uint64_t order_book_rows_unwritten{0};
    std::size_t slot_high_water{0};
    std::uint64_t byte_high_water{0};
    std::uint64_t age_flushes{0};
    std::uint64_t overflow_batches_published{0};
    std::uint64_t overflow_capacity_bytes{0};
    std::uint64_t overflow_capacity_high_water{0};
    std::uint64_t overflow_release_failures{0};
};

struct CsvWriterNotifications {
    void* context{nullptr};
    void (*notify_resume)(void*) noexcept{nullptr};
    void (*notify_failure)(void*) noexcept{nullptr};
    // Optional lifetime token for a callback bridge shared with the writer.
    // It prevents a cross-thread notification from targeting destroyed
    // controller state while shutdown joins the consumer.
    std::shared_ptr<void> lifetime{};

    [[nodiscard]] bool has_resume() const noexcept {
        return context != nullptr && notify_resume != nullptr;
    }

    [[nodiscard]] bool has_failure() const noexcept {
        return context != nullptr && notify_failure != nullptr;
    }
};

struct CsvWriterResult {
    OutputSetWriteError output_error{};
    CsvWriterMetrics metrics{};

    [[nodiscard]] bool success() const noexcept {
        return !output_error &&
               metrics.audit_rows_unwritten == 0U &&
               metrics.order_book_rows_unwritten == 0U;
    }
};

class CsvWriter {
public:
    // Takes exclusive ownership of an initialized output set and starts one
    // writer thread. Producer methods must be called by one processing
    // thread. close() and join() are idempotent; join() implies close().
    [[nodiscard]] static std::unique_ptr<CsvWriter> start(
        std::unique_ptr<CsvOutputSet> output,
        CsvWriterCreateError& error,
        CsvWriterNotifications notifications = {}) noexcept;

    CsvWriter(const CsvWriter&) = delete;
    CsvWriter& operator=(const CsvWriter&) = delete;
    CsvWriter(CsvWriter&&) = delete;
    CsvWriter& operator=(CsvWriter&&) = delete;

    ~CsvWriter() noexcept;

    // The returned batch is owned by the current ring slot and remains valid
    // until publish() or cancel(). It can be passed directly to
    // LiveEventPipeline::process().
    [[nodiscard]] EventRowBatch* try_acquire(
        CsvWriterAcquireError& error) noexcept;
    [[nodiscard]] CsvWriterPublishError publish(
        std::size_t target_index) noexcept;
    void cancel() noexcept;

    [[nodiscard]] CsvWriterOccupancy occupancy() const noexcept;
    [[nodiscard]] bool should_pause() const noexcept;
    [[nodiscard]] bool below_resume_watermark() const noexcept;
    // Arms one notification for the next observation below the low-water
    // mark. The callback may run synchronously here or on the writer thread;
    // it must therefore be noexcept and only hand work to the I/O owner.
    void arm_resume_notification() noexcept;
    [[nodiscard]] bool failed() const noexcept;

    void close() noexcept;
    [[nodiscard]] CsvWriterResult join() noexcept;

private:
    struct Impl;

    CsvWriter(
        std::unique_ptr<CsvOutputSet> output,
        CsvWriterNotifications notifications);

    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const CsvWriterCreateError error) noexcept {
    switch (error) {
        case CsvWriterCreateError::none:
            return "none";
        case CsvWriterCreateError::invalid_output:
            return "invalid_output";
        case CsvWriterCreateError::allocation_failure:
            return "allocation_failure";
        case CsvWriterCreateError::thread_start_failure:
            return "thread_start_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const CsvWriterAcquireError error) noexcept {
    switch (error) {
        case CsvWriterAcquireError::none:
            return "none";
        case CsvWriterAcquireError::closed:
            return "closed";
        case CsvWriterAcquireError::failed:
            return "failed";
        case CsvWriterAcquireError::already_acquired:
            return "already_acquired";
        case CsvWriterAcquireError::slot_capacity:
            return "slot_capacity";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const CsvWriterPublishError error) noexcept {
    switch (error) {
        case CsvWriterPublishError::none:
            return "none";
        case CsvWriterPublishError::no_acquired_slot:
            return "no_acquired_slot";
        case CsvWriterPublishError::closed:
            return "closed";
        case CsvWriterPublishError::failed:
            return "failed";
        case CsvWriterPublishError::invalid_target:
            return "invalid_target";
        case CsvWriterPublishError::invalid_batch:
            return "invalid_batch";
        case CsvWriterPublishError::byte_capacity:
            return "byte_capacity";
        case CsvWriterPublishError::counter_overflow:
            return "counter_overflow";
    }
    return "unknown";
}

}  // namespace hft
