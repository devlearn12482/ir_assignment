#pragma once

#include "hft/csv_formatter.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace hft {

inline constexpr std::size_t kCsvAggregationBufferBytes{256U * 1024U};

enum class CsvFileKind : std::uint8_t {
    market_data,
    order_book,
};

enum class FileSinkErrorCode : std::uint8_t {
    none,
    invalid_path,
    invalid_file_kind,
    invalid_record,
    allocation_failure,
    open_failed,
    write_failed,
    close_failed,
};

enum class FileSinkCleanupOperation : std::uint8_t {
    none,
    close,
    remove,
};

struct FileSinkError {
    FileSinkErrorCode code{FileSinkErrorCode::none};
    int native_error{0};
    FileSinkCleanupOperation cleanup_operation{
        FileSinkCleanupOperation::none};
    int cleanup_native_error{0};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != FileSinkErrorCode::none;
    }
};

struct FileOpenResult {
    int descriptor{-1};
    int native_error{0};
};

struct FileWriteResult {
    std::ptrdiff_t bytes{-1};
    int native_error{0};
};

struct FileCloseResult {
    bool success{false};
    int native_error{0};
};

struct FileRemoveResult {
    bool success{false};
    int native_error{0};
};

class FileOperations {
public:
    virtual ~FileOperations() = default;

    [[nodiscard]] virtual FileOpenResult open_exclusive(
        const std::string& path) noexcept = 0;
    [[nodiscard]] virtual FileWriteResult write(
        int descriptor,
        const char* data,
        std::size_t size) noexcept = 0;
    [[nodiscard]] virtual FileCloseResult close(
        int descriptor) noexcept = 0;
    [[nodiscard]] virtual FileRemoveResult remove(
        const std::string& path) noexcept = 0;
};

[[nodiscard]] std::shared_ptr<FileOperations> posix_file_operations();

struct BufferedFileSinkMetrics {
    std::uint64_t native_write_calls{0};
    std::uint64_t bytes_written{0};
    std::uint64_t rows_written{0};
    std::uint64_t buffer_flushes{0};
    std::uint64_t direct_large_records{0};
};

class BufferedCsvFileSink {
public:
    // Single-owner boundary: open writes the exact selected header before
    // returning, append/flush/close are not thread-safe, and flush transfers
    // bytes only to the kernel page cache (it does not call fsync).
    [[nodiscard]] static std::unique_ptr<BufferedCsvFileSink>
    open_exclusive(
        std::string path,
        CsvFileKind kind,
        FileSinkError& error) noexcept;

    [[nodiscard]] static std::unique_ptr<BufferedCsvFileSink>
    open_exclusive(
        std::string path,
        CsvFileKind kind,
        std::shared_ptr<FileOperations> operations,
        FileSinkError& error) noexcept;

    BufferedCsvFileSink(const BufferedCsvFileSink&) = delete;
    BufferedCsvFileSink& operator=(const BufferedCsvFileSink&) = delete;
    BufferedCsvFileSink(BufferedCsvFileSink&&) = delete;
    BufferedCsvFileSink& operator=(BufferedCsvFileSink&&) = delete;

    ~BufferedCsvFileSink() noexcept;

    [[nodiscard]] FileSinkError append_record(
        std::string_view record) noexcept;
    [[nodiscard]] FileSinkError flush() noexcept;
    [[nodiscard]] FileSinkError close() noexcept;

    [[nodiscard]] bool is_open() const noexcept;
    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] std::size_t buffered_bytes() const noexcept;
    [[nodiscard]] std::uint64_t buffered_rows() const noexcept;
    [[nodiscard]] const std::string& path() const noexcept;
    [[nodiscard]] const FileSinkError& first_error() const noexcept;
    [[nodiscard]] const BufferedFileSinkMetrics& metrics() const noexcept;

private:
    BufferedCsvFileSink(
        std::string path,
        int descriptor,
        std::shared_ptr<FileOperations> operations);

    [[nodiscard]] FileSinkError flush_buffer() noexcept;
    [[nodiscard]] FileSinkError write_all(
        std::string_view bytes) noexcept;
    [[nodiscard]] FileSinkError latch_error(
        FileSinkErrorCode code,
        int native_error) noexcept;
    [[nodiscard]] FileSinkError close_after_failed_open() noexcept;

    std::string path_;
    int descriptor_{-1};
    std::shared_ptr<FileOperations> operations_;
    std::unique_ptr<char[]> buffer_;
    std::size_t buffered_bytes_{0};
    std::uint64_t buffered_rows_{0};
    FileSinkError first_error_{};
    BufferedFileSinkMetrics metrics_{};
};

[[nodiscard]] constexpr std::string_view to_string(
    const FileSinkErrorCode code) noexcept {
    switch (code) {
        case FileSinkErrorCode::none:
            return "none";
        case FileSinkErrorCode::invalid_path:
            return "invalid_path";
        case FileSinkErrorCode::invalid_file_kind:
            return "invalid_file_kind";
        case FileSinkErrorCode::invalid_record:
            return "invalid_record";
        case FileSinkErrorCode::allocation_failure:
            return "allocation_failure";
        case FileSinkErrorCode::open_failed:
            return "open_failed";
        case FileSinkErrorCode::write_failed:
            return "write_failed";
        case FileSinkErrorCode::close_failed:
            return "close_failed";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const FileSinkCleanupOperation operation) noexcept {
    switch (operation) {
        case FileSinkCleanupOperation::none:
            return "none";
        case FileSinkCleanupOperation::close:
            return "close";
        case FileSinkCleanupOperation::remove:
            return "remove";
    }
    return "unknown";
}

}  // namespace hft
