#pragma once

#include "hft/buffered_file_sink.h"
#include "hft/event_processor.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

namespace hft {

enum class CsvOutputMode : std::uint8_t {
    live_capture,
    replay,
};

struct ReplayOutputSpec {
    std::string_view normalized_symbol{};
    std::string_view input_file_stem{};
};

enum class OutputRollbackOperation : std::uint8_t {
    none,
    close_file,
    remove_file,
    remove_directory,
};

struct OutputRollbackError {
    OutputRollbackOperation operation{OutputRollbackOperation::none};
    std::size_t target_index{kNoSymbolIndex};
    CsvFileKind file_kind{CsvFileKind::market_data};
    FileSinkError file_error{};
    int native_error{0};

    [[nodiscard]] explicit operator bool() const noexcept {
        return operation != OutputRollbackOperation::none;
    }
};

enum class OutputSetOpenErrorCode : std::uint8_t {
    none,
    invalid_input,
    invalid_venue,
    invalid_utc_date,
    invalid_replay_stem,
    duplicate_output_path,
    invalid_output_directory,
    output_directory_not_empty,
    file_system_failure,
    allocation_failure,
    file_sink_failure,
};

struct OutputSetOpenError {
    OutputSetOpenErrorCode code{OutputSetOpenErrorCode::none};
    SymbolValidationError symbol_error{SymbolValidationError::none};
    std::size_t target_index{kNoSymbolIndex};
    CsvFileKind file_kind{CsvFileKind::market_data};
    FileSinkError file_error{};
    int native_error{0};
    std::string path{};
    OutputRollbackError rollback_error{};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != OutputSetOpenErrorCode::none;
    }
};

enum class OutputSetWriteErrorCode : std::uint8_t {
    none,
    invalid_target,
    invalid_batch,
    closed,
    file_failure,
};

struct OutputSetWriteError {
    OutputSetWriteErrorCode code{OutputSetWriteErrorCode::none};
    std::size_t target_index{kNoSymbolIndex};
    CsvFileKind file_kind{CsvFileKind::market_data};
    FileSinkError file_error{};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != OutputSetWriteErrorCode::none;
    }
};

struct CsvOutputSetMetrics {
    std::uint64_t audit_rows_written{0};
    std::uint64_t order_book_rows_written{0};
    std::uint64_t audit_rows_buffered{0};
    std::uint64_t order_book_rows_buffered{0};
};

class CsvOutputSet {
public:
    // Single-owner file boundary. Resolve a symbol once to its stable target
    // index; the processing/writer path routes batches by that index without
    // rebuilding filenames or searching symbols on every event.
    [[nodiscard]] static std::unique_ptr<CsvOutputSet> open_live(
        std::string output_directory,
        PayloadVenue venue,
        const std::string_view* normalized_symbols,
        std::size_t symbol_count,
        std::string utc_date,
        OutputSetOpenError& error) noexcept;

    [[nodiscard]] static std::unique_ptr<CsvOutputSet> open_live(
        std::string output_directory,
        PayloadVenue venue,
        const std::string_view* normalized_symbols,
        std::size_t symbol_count,
        std::string utc_date,
        std::shared_ptr<FileOperations> operations,
        OutputSetOpenError& error) noexcept;

    [[nodiscard]] static std::unique_ptr<CsvOutputSet> open_replay(
        std::string output_directory,
        const ReplayOutputSpec* specifications,
        std::size_t specification_count,
        OutputSetOpenError& error) noexcept;

    [[nodiscard]] static std::unique_ptr<CsvOutputSet> open_replay(
        std::string output_directory,
        const ReplayOutputSpec* specifications,
        std::size_t specification_count,
        std::shared_ptr<FileOperations> operations,
        OutputSetOpenError& error) noexcept;

    CsvOutputSet(const CsvOutputSet&) = delete;
    CsvOutputSet& operator=(const CsvOutputSet&) = delete;
    CsvOutputSet(CsvOutputSet&&) = delete;
    CsvOutputSet& operator=(CsvOutputSet&&) = delete;

    ~CsvOutputSet() noexcept;

    [[nodiscard]] std::size_t target_count() const noexcept;
    [[nodiscard]] CsvOutputMode mode() const noexcept;
    [[nodiscard]] std::optional<std::size_t> find_target(
        std::string_view normalized_symbol) const noexcept;
    [[nodiscard]] std::string_view symbol(
        std::size_t target_index) const noexcept;
    [[nodiscard]] std::string_view audit_path(
        std::size_t target_index) const noexcept;
    [[nodiscard]] std::string_view order_book_path(
        std::size_t target_index) const noexcept;

    [[nodiscard]] OutputSetWriteError write_batch(
        std::size_t target_index,
        const EventRowBatch& batch) noexcept;
    [[nodiscard]] OutputSetWriteError flush_all() noexcept;
    [[nodiscard]] OutputSetWriteError close_all() noexcept;

    [[nodiscard]] bool failed() const noexcept;
    [[nodiscard]] bool closed() const noexcept;
    [[nodiscard]] const OutputSetWriteError& first_error() const noexcept;
    [[nodiscard]] CsvOutputSetMetrics metrics() const noexcept;

private:
    struct TargetFiles {
        std::string symbol{};
        std::string audit_path{};
        std::string order_book_path{};
        std::unique_ptr<BufferedCsvFileSink> audit{};
        std::unique_ptr<BufferedCsvFileSink> order_book{};
        bool audit_created{false};
        bool order_book_created{false};
    };

    CsvOutputSet(
        CsvOutputMode mode,
        std::shared_ptr<FileOperations> operations) noexcept;

    [[nodiscard]] static bool valid_utc_date(
        std::string_view value) noexcept;
    [[nodiscard]] static bool valid_replay_stem(
        std::string_view value) noexcept;
    [[nodiscard]] static std::string_view venue_name(
        PayloadVenue venue) noexcept;

    [[nodiscard]] bool prepare_output_directory(
        OutputSetOpenError& error);
    [[nodiscard]] bool open_files(
        OutputSetOpenError& error);
    void rollback_initialization(OutputSetOpenError& error) noexcept;
    void record_rollback_error(
        OutputSetOpenError& error,
        OutputRollbackOperation operation,
        std::size_t target_index,
        CsvFileKind file_kind,
        FileSinkError file_error,
        int native_error) noexcept;
    [[nodiscard]] bool valid_batch_contract(
        const EventRowBatch& batch) const noexcept;
    [[nodiscard]] OutputSetWriteError latch_error(
        OutputSetWriteErrorCode code,
        std::size_t target_index,
        CsvFileKind file_kind,
        FileSinkError file_error = {}) noexcept;

    CsvOutputMode mode_{CsvOutputMode::live_capture};
    std::shared_ptr<FileOperations> operations_{};
    std::string output_directory_{};
    std::array<TargetFiles, kMaxConfiguredSymbols> targets_{};
    std::size_t target_count_{0};
    bool directory_created_{false};
    bool closed_{false};
    OutputSetWriteError first_error_{};
};

[[nodiscard]] constexpr std::string_view to_string(
    const OutputSetOpenErrorCode code) noexcept {
    switch (code) {
        case OutputSetOpenErrorCode::none:
            return "none";
        case OutputSetOpenErrorCode::invalid_input:
            return "invalid_input";
        case OutputSetOpenErrorCode::invalid_venue:
            return "invalid_venue";
        case OutputSetOpenErrorCode::invalid_utc_date:
            return "invalid_utc_date";
        case OutputSetOpenErrorCode::invalid_replay_stem:
            return "invalid_replay_stem";
        case OutputSetOpenErrorCode::duplicate_output_path:
            return "duplicate_output_path";
        case OutputSetOpenErrorCode::invalid_output_directory:
            return "invalid_output_directory";
        case OutputSetOpenErrorCode::output_directory_not_empty:
            return "output_directory_not_empty";
        case OutputSetOpenErrorCode::file_system_failure:
            return "file_system_failure";
        case OutputSetOpenErrorCode::allocation_failure:
            return "allocation_failure";
        case OutputSetOpenErrorCode::file_sink_failure:
            return "file_sink_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const OutputSetWriteErrorCode code) noexcept {
    switch (code) {
        case OutputSetWriteErrorCode::none:
            return "none";
        case OutputSetWriteErrorCode::invalid_target:
            return "invalid_target";
        case OutputSetWriteErrorCode::invalid_batch:
            return "invalid_batch";
        case OutputSetWriteErrorCode::closed:
            return "closed";
        case OutputSetWriteErrorCode::file_failure:
            return "file_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const OutputRollbackOperation operation) noexcept {
    switch (operation) {
        case OutputRollbackOperation::none:
            return "none";
        case OutputRollbackOperation::close_file:
            return "close_file";
        case OutputRollbackOperation::remove_file:
            return "remove_file";
        case OutputRollbackOperation::remove_directory:
            return "remove_directory";
    }
    return "unknown";
}

}  // namespace hft
