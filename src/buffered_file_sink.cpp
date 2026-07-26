#include "hft/buffered_file_sink.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

namespace hft {
namespace {

class PosixFileOperations final : public FileOperations {
public:
    [[nodiscard]] FileOpenResult open_exclusive(
        const std::string& path) noexcept override {
        const int descriptor = ::open(
            path.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC,
            S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        if (descriptor < 0) {
            return FileOpenResult{-1, errno};
        }
        return FileOpenResult{descriptor, 0};
    }

    [[nodiscard]] FileWriteResult write(
        const int descriptor,
        const char* const data,
        const std::size_t size) noexcept override {
        const ssize_t result = ::write(descriptor, data, size);
        if (result < 0) {
            return FileWriteResult{-1, errno};
        }
        return FileWriteResult{
            static_cast<std::ptrdiff_t>(result), 0};
    }

    [[nodiscard]] FileCloseResult close(
        const int descriptor) noexcept override {
        if (::close(descriptor) != 0) {
            return FileCloseResult{false, errno};
        }
        return FileCloseResult{true, 0};
    }

    [[nodiscard]] FileRemoveResult remove(
        const std::string& path) noexcept override {
        if (::unlink(path.c_str()) != 0) {
            return FileRemoveResult{false, errno};
        }
        return FileRemoveResult{true, 0};
    }
};

[[nodiscard]] constexpr std::string_view header_for(
    const CsvFileKind kind) noexcept {
    switch (kind) {
        case CsvFileKind::market_data:
            return kMarketDataCsvHeader;
        case CsvFileKind::order_book:
            return kOrderBookCsvHeader;
    }
    return {};
}

}  // namespace

std::shared_ptr<FileOperations> posix_file_operations() {
    static const std::shared_ptr<FileOperations> operations{
        std::make_shared<PosixFileOperations>()};
    return operations;
}

BufferedCsvFileSink::BufferedCsvFileSink(
    std::string path,
    const int descriptor,
    std::shared_ptr<FileOperations> operations)
    : path_{std::move(path)},
      descriptor_{descriptor},
      operations_{std::move(operations)},
      buffer_{std::make_unique<char[]>(kCsvAggregationBufferBytes)} {}

std::unique_ptr<BufferedCsvFileSink>
BufferedCsvFileSink::open_exclusive(
    std::string path,
    const CsvFileKind kind,
    FileSinkError& error) noexcept {
    try {
        return open_exclusive(
            std::move(path), kind, posix_file_operations(), error);
    } catch (const std::bad_alloc&) {
        error = FileSinkError{
            FileSinkErrorCode::allocation_failure, 0};
        return nullptr;
    }
}

std::unique_ptr<BufferedCsvFileSink>
BufferedCsvFileSink::open_exclusive(
    std::string path,
    const CsvFileKind kind,
    std::shared_ptr<FileOperations> operations,
    FileSinkError& error) noexcept {
    error = {};
    if (path.empty()) {
        error = FileSinkError{FileSinkErrorCode::invalid_path, 0};
        return nullptr;
    }
    if (!operations) {
        error = FileSinkError{
            FileSinkErrorCode::allocation_failure, 0};
        return nullptr;
    }

    const std::string_view header = header_for(kind);
    if (header.empty()) {
        error = FileSinkError{
            FileSinkErrorCode::invalid_file_kind, 0};
        return nullptr;
    }

    const FileOpenResult opened = operations->open_exclusive(path);
    if (opened.descriptor < 0) {
        error = FileSinkError{
            FileSinkErrorCode::open_failed, opened.native_error};
        return nullptr;
    }

    std::unique_ptr<BufferedCsvFileSink> sink;
    try {
        sink.reset(new BufferedCsvFileSink{
            path, opened.descriptor, operations});
    } catch (const std::bad_alloc&) {
        error = FileSinkError{
            FileSinkErrorCode::allocation_failure, 0};
        const FileCloseResult close_result =
            operations->close(opened.descriptor);
        const FileRemoveResult remove_result =
            operations->remove(path);
        if (!close_result.success) {
            error.cleanup_operation =
                FileSinkCleanupOperation::close;
            error.cleanup_native_error =
                close_result.native_error;
        } else if (!remove_result.success &&
                   remove_result.native_error != ENOENT) {
            error.cleanup_operation =
                FileSinkCleanupOperation::remove;
            error.cleanup_native_error =
                remove_result.native_error;
        }
        return nullptr;
    }

    const FileSinkError header_error = sink->write_all(header);
    if (header_error) {
        error = header_error;
        const FileSinkError cleanup =
            sink->close_after_failed_open();
        error.cleanup_operation = cleanup.cleanup_operation;
        error.cleanup_native_error =
            cleanup.cleanup_native_error;
        return nullptr;
    }
    return sink;
}

BufferedCsvFileSink::~BufferedCsvFileSink() noexcept {
    if (descriptor_ >= 0) {
        static_cast<void>(close());
    }
}

FileSinkError BufferedCsvFileSink::append_record(
    const std::string_view record) noexcept {
    if (first_error_) {
        return first_error_;
    }
    if (descriptor_ < 0 || record.empty() ||
        record.size() > kMaxCsvRecordBytes ||
        record.back() != '\n') {
        return latch_error(FileSinkErrorCode::invalid_record, 0);
    }

    if (record.size() > kCsvAggregationBufferBytes) {
        const FileSinkError flush_error = flush_buffer();
        if (flush_error) {
            return flush_error;
        }
        const FileSinkError write_error = write_all(record);
        if (write_error) {
            return write_error;
        }
        ++metrics_.rows_written;
        ++metrics_.direct_large_records;
        return {};
    }

    if (record.size() >
        kCsvAggregationBufferBytes - buffered_bytes_) {
        const FileSinkError flush_error = flush_buffer();
        if (flush_error) {
            return flush_error;
        }
    }

    std::memcpy(
        buffer_.get() + buffered_bytes_,
        record.data(),
        record.size());
    buffered_bytes_ += record.size();
    ++buffered_rows_;
    return {};
}

FileSinkError BufferedCsvFileSink::flush() noexcept {
    if (first_error_) {
        return first_error_;
    }
    if (descriptor_ < 0) {
        return latch_error(FileSinkErrorCode::write_failed, EBADF);
    }
    return flush_buffer();
}

FileSinkError BufferedCsvFileSink::close() noexcept {
    if (descriptor_ < 0) {
        return first_error_;
    }

    if (!first_error_) {
        static_cast<void>(flush_buffer());
    }

    const int descriptor = descriptor_;
    descriptor_ = -1;
    const FileCloseResult close_result =
        operations_->close(descriptor);
    if (!close_result.success && !first_error_) {
        static_cast<void>(latch_error(
            FileSinkErrorCode::close_failed,
            close_result.native_error));
    }
    return first_error_;
}

bool BufferedCsvFileSink::is_open() const noexcept {
    return descriptor_ >= 0;
}

bool BufferedCsvFileSink::failed() const noexcept {
    return static_cast<bool>(first_error_);
}

std::size_t BufferedCsvFileSink::buffered_bytes() const noexcept {
    return buffered_bytes_;
}

std::uint64_t BufferedCsvFileSink::buffered_rows() const noexcept {
    return buffered_rows_;
}

const std::string& BufferedCsvFileSink::path() const noexcept {
    return path_;
}

const FileSinkError& BufferedCsvFileSink::first_error() const noexcept {
    return first_error_;
}

const BufferedFileSinkMetrics&
BufferedCsvFileSink::metrics() const noexcept {
    return metrics_;
}

FileSinkError BufferedCsvFileSink::flush_buffer() noexcept {
    if (buffered_bytes_ == 0U) {
        return {};
    }

    const FileSinkError write_error = write_all(
        std::string_view{buffer_.get(), buffered_bytes_});
    if (write_error) {
        return write_error;
    }

    metrics_.rows_written += buffered_rows_;
    ++metrics_.buffer_flushes;
    buffered_bytes_ = 0U;
    buffered_rows_ = 0U;
    return {};
}

FileSinkError BufferedCsvFileSink::write_all(
    const std::string_view bytes) noexcept {
    std::size_t offset{0};
    while (offset < bytes.size()) {
        const FileWriteResult result = operations_->write(
            descriptor_, bytes.data() + offset, bytes.size() - offset);
        ++metrics_.native_write_calls;
        if (result.bytes < 0) {
            if (result.native_error == EINTR) {
                continue;
            }
            return latch_error(
                FileSinkErrorCode::write_failed,
                result.native_error);
        }
        if (result.bytes == 0) {
            return latch_error(FileSinkErrorCode::write_failed, EIO);
        }

        const auto accepted =
            static_cast<std::size_t>(result.bytes);
        if (accepted > bytes.size() - offset) {
            return latch_error(FileSinkErrorCode::write_failed, EIO);
        }
        offset += accepted;
        metrics_.bytes_written += accepted;
    }
    return {};
}

FileSinkError BufferedCsvFileSink::latch_error(
    const FileSinkErrorCode code,
    const int native_error) noexcept {
    if (!first_error_) {
        first_error_ = FileSinkError{code, native_error};
    }
    return first_error_;
}

FileSinkError BufferedCsvFileSink::close_after_failed_open() noexcept {
    const int descriptor = descriptor_;
    descriptor_ = -1;
    const FileCloseResult close_result =
        operations_->close(descriptor);
    const FileRemoveResult remove_result =
        operations_->remove(path_);
    if (!close_result.success) {
        return FileSinkError{
            FileSinkErrorCode::none,
            0,
            FileSinkCleanupOperation::close,
            close_result.native_error};
    }
    if (!remove_result.success &&
        remove_result.native_error != ENOENT) {
        return FileSinkError{
            FileSinkErrorCode::none,
            0,
            FileSinkCleanupOperation::remove,
            remove_result.native_error};
    }
    return {};
}

}  // namespace hft
