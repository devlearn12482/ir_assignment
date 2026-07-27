#include "hft/csv_output_set.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <filesystem>
#include <new>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace hft {
namespace {

[[nodiscard]] bool is_ascii_digit(const char value) noexcept {
    return value >= '0' && value <= '9';
}

[[nodiscard]] unsigned parse_two_digits(
    const std::string_view value,
    const std::size_t offset) noexcept {
    return static_cast<unsigned>(value[offset] - '0') * 10U +
           static_cast<unsigned>(value[offset + 1U] - '0');
}

[[nodiscard]] unsigned parse_four_digits(
    const std::string_view value) noexcept {
    return static_cast<unsigned>(value[0] - '0') * 1000U +
           static_cast<unsigned>(value[1] - '0') * 100U +
           static_cast<unsigned>(value[2] - '0') * 10U +
           static_cast<unsigned>(value[3] - '0');
}

[[nodiscard]] bool is_leap_year(const unsigned year) noexcept {
    return year % 4U == 0U &&
           (year % 100U != 0U || year % 400U == 0U);
}

[[nodiscard]] unsigned days_in_month(
    const unsigned year,
    const unsigned month) noexcept {
    constexpr std::array<unsigned, 12U> days{
        31U, 28U, 31U, 30U, 31U, 30U,
        31U, 31U, 30U, 31U, 30U, 31U};
    if (month == 0U || month > days.size()) {
        return 0U;
    }
    if (month == 2U && is_leap_year(year)) {
        return 29U;
    }
    return days[month - 1U];
}

void set_allocation_error(OutputSetOpenError& error) {
    error = {};
    error.code = OutputSetOpenErrorCode::allocation_failure;
}

}  // namespace

CsvOutputSet::CsvOutputSet(
    const CsvOutputMode mode,
    std::shared_ptr<FileOperations> operations) noexcept
    : mode_{mode}, operations_{std::move(operations)} {}

std::unique_ptr<CsvOutputSet> CsvOutputSet::open_live(
    std::string output_directory,
    const PayloadVenue venue,
    const std::string_view* const normalized_symbols,
    const std::size_t symbol_count,
    std::string utc_date,
    OutputSetOpenError& error) noexcept {
    try {
        return open_live(
            std::move(output_directory),
            venue,
            normalized_symbols,
            symbol_count,
            std::move(utc_date),
            posix_file_operations(),
            error);
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        return nullptr;
    }
}

std::unique_ptr<CsvOutputSet> CsvOutputSet::open_live(
    std::string output_directory,
    const PayloadVenue venue,
    const std::string_view* const normalized_symbols,
    const std::size_t symbol_count,
    std::string utc_date,
    std::shared_ptr<FileOperations> operations,
    OutputSetOpenError& error) noexcept {
    error = {};
    const SymbolSetValidationResult symbols =
        validate_symbol_set(normalized_symbols, symbol_count);
    if (!symbols) {
        error.code = OutputSetOpenErrorCode::invalid_input;
        error.symbol_error = symbols.error;
        error.target_index = symbols.first_index;
        return nullptr;
    }
    const std::string_view venue_text = venue_name(venue);
    if (venue_text.empty()) {
        error.code = OutputSetOpenErrorCode::invalid_venue;
        return nullptr;
    }
    if (!valid_utc_date(utc_date)) {
        error.code = OutputSetOpenErrorCode::invalid_utc_date;
        return nullptr;
    }
    if (output_directory.empty() || !operations) {
        error.code = OutputSetOpenErrorCode::invalid_input;
        return nullptr;
    }

    std::unique_ptr<CsvOutputSet> output;
    try {
        output.reset(new CsvOutputSet{
            CsvOutputMode::live_capture, operations});
        output->output_directory_ = std::move(output_directory);
        output->target_count_ = symbol_count;
        for (std::size_t index = 0; index < symbol_count; ++index) {
            TargetFiles& target = output->targets_[index];
            target.symbol = normalized_symbols[index];
            const std::string stem =
                "market_data_" + std::string{venue_text} + "_" +
                target.symbol + "_" + utc_date;
            const std::filesystem::path base{
                output->output_directory_};
            target.audit_path =
                (base / (stem + ".csv")).string();
            target.order_book_path =
                (base / (stem + "_orderbook.csv")).string();
        }
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        return nullptr;
    }

    try {
        if (!output->prepare_output_directory(error) ||
            !output->open_files(error)) {
            output->rollback_initialization(error);
            return nullptr;
        }
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        output->rollback_initialization(error);
        return nullptr;
    }
    return output;
}

std::unique_ptr<CsvOutputSet> CsvOutputSet::open_replay(
    std::string output_directory,
    const ReplayOutputSpec* const specifications,
    const std::size_t specification_count,
    OutputSetOpenError& error) noexcept {
    try {
        return open_replay(
            std::move(output_directory),
            specifications,
            specification_count,
            posix_file_operations(),
            error);
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        return nullptr;
    }
}

std::unique_ptr<CsvOutputSet> CsvOutputSet::open_replay(
    std::string output_directory,
    const ReplayOutputSpec* const specifications,
    const std::size_t specification_count,
    std::shared_ptr<FileOperations> operations,
    OutputSetOpenError& error) noexcept {
    error = {};
    if (specifications == nullptr || specification_count == 0U ||
        specification_count > kMaxConfiguredSymbols ||
        output_directory.empty() || !operations) {
        error.code = OutputSetOpenErrorCode::invalid_input;
        return nullptr;
    }

    std::array<std::string_view, kMaxConfiguredSymbols> symbols{};
    for (std::size_t index = 0; index < specification_count; ++index) {
        symbols[index] = specifications[index].normalized_symbol;
    }
    const SymbolSetValidationResult symbol_validation =
        validate_symbol_set(symbols.data(), specification_count);
    if (!symbol_validation) {
        error.code = OutputSetOpenErrorCode::invalid_input;
        error.symbol_error = symbol_validation.error;
        error.target_index = symbol_validation.first_index;
        return nullptr;
    }

    for (std::size_t index = 0; index < specification_count; ++index) {
        if (!valid_replay_stem(
                specifications[index].input_file_stem)) {
            error.code =
                OutputSetOpenErrorCode::invalid_replay_stem;
            error.target_index = index;
            return nullptr;
        }
        for (std::size_t previous = 0; previous < index; ++previous) {
            if (specifications[index].input_file_stem ==
                specifications[previous].input_file_stem) {
                error.code =
                    OutputSetOpenErrorCode::duplicate_output_path;
                error.target_index = index;
                return nullptr;
            }
        }
    }

    std::unique_ptr<CsvOutputSet> output;
    try {
        output.reset(new CsvOutputSet{
            CsvOutputMode::replay, operations});
        output->output_directory_ = std::move(output_directory);
        output->target_count_ = specification_count;
        const std::filesystem::path base{
            output->output_directory_};
        for (std::size_t index = 0;
             index < specification_count;
             ++index) {
            TargetFiles& target = output->targets_[index];
            target.symbol =
                specifications[index].normalized_symbol;
            target.order_book_path =
                (base /
                 (std::string{
                      specifications[index].input_file_stem} +
                  "_orderbook.csv"))
                    .string();
        }
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        return nullptr;
    }

    try {
        if (!output->prepare_output_directory(error) ||
            !output->open_files(error)) {
            output->rollback_initialization(error);
            return nullptr;
        }
    } catch (const std::bad_alloc&) {
        set_allocation_error(error);
        output->rollback_initialization(error);
        return nullptr;
    }
    return output;
}

CsvOutputSet::~CsvOutputSet() noexcept {
    static_cast<void>(close_all());
}

std::size_t CsvOutputSet::target_count() const noexcept {
    return target_count_;
}

CsvOutputMode CsvOutputSet::mode() const noexcept {
    return mode_;
}

std::optional<std::size_t> CsvOutputSet::find_target(
    const std::string_view normalized_symbol) const noexcept {
    for (std::size_t index = 0; index < target_count_; ++index) {
        if (targets_[index].symbol == normalized_symbol) {
            return index;
        }
    }
    return std::nullopt;
}

std::string_view CsvOutputSet::symbol(
    const std::size_t target_index) const noexcept {
    if (target_index >= target_count_) {
        return {};
    }
    return targets_[target_index].symbol;
}

std::string_view CsvOutputSet::audit_path(
    const std::size_t target_index) const noexcept {
    if (target_index >= target_count_) {
        return {};
    }
    return targets_[target_index].audit_path;
}

std::string_view CsvOutputSet::order_book_path(
    const std::size_t target_index) const noexcept {
    if (target_index >= target_count_) {
        return {};
    }
    return targets_[target_index].order_book_path;
}

OutputSetWriteError CsvOutputSet::write_batch(
    const std::size_t target_index,
    const EventRowBatch& batch) noexcept {
    if (first_error_) {
        return first_error_;
    }
    if (closed_) {
        return latch_error(
            OutputSetWriteErrorCode::closed,
            target_index,
            CsvFileKind::market_data);
    }
    if (target_index >= target_count_) {
        return latch_error(
            OutputSetWriteErrorCode::invalid_target,
            target_index,
            CsvFileKind::market_data);
    }
    if (!valid_batch_contract(batch)) {
        return latch_error(
            OutputSetWriteErrorCode::invalid_batch,
            target_index,
            CsvFileKind::market_data);
    }

    TargetFiles& target = targets_[target_index];
    if (batch.has_audit_row) {
        const FileSinkError file_error =
            target.audit->append_record(batch.audit_row.view());
        if (file_error) {
            return latch_error(
                OutputSetWriteErrorCode::file_failure,
                target_index,
                CsvFileKind::market_data,
                file_error);
        }
    }
    if (batch.has_order_book_row) {
        const FileSinkError file_error =
            target.order_book->append_record(
                batch.order_book_row.view());
        if (file_error) {
            return latch_error(
                OutputSetWriteErrorCode::file_failure,
                target_index,
                CsvFileKind::order_book,
                file_error);
        }
    }
    return {};
}

OutputSetWriteError CsvOutputSet::flush_all() noexcept {
    if (closed_) {
        if (first_error_) {
            return first_error_;
        }
        return latch_error(
            OutputSetWriteErrorCode::closed,
            kNoSymbolIndex,
            CsvFileKind::market_data);
    }

    for (std::size_t index = 0; index < target_count_; ++index) {
        TargetFiles& target = targets_[index];
        if (target.audit) {
            const FileSinkError error = target.audit->flush();
            if (error && !first_error_) {
                static_cast<void>(latch_error(
                    OutputSetWriteErrorCode::file_failure,
                    index,
                    CsvFileKind::market_data,
                    error));
            }
        }
        if (target.order_book) {
            const FileSinkError error =
                target.order_book->flush();
            if (error && !first_error_) {
                static_cast<void>(latch_error(
                    OutputSetWriteErrorCode::file_failure,
                    index,
                    CsvFileKind::order_book,
                    error));
            }
        }
    }
    return first_error_;
}

OutputSetWriteError CsvOutputSet::close_all() noexcept {
    if (closed_) {
        return first_error_;
    }

    for (std::size_t index = 0; index < target_count_; ++index) {
        TargetFiles& target = targets_[index];
        if (target.audit) {
            const FileSinkError error = target.audit->close();
            if (error && !first_error_) {
                static_cast<void>(latch_error(
                    OutputSetWriteErrorCode::file_failure,
                    index,
                    CsvFileKind::market_data,
                    error));
            }
        }
        if (target.order_book) {
            const FileSinkError error =
                target.order_book->close();
            if (error && !first_error_) {
                static_cast<void>(latch_error(
                    OutputSetWriteErrorCode::file_failure,
                    index,
                    CsvFileKind::order_book,
                    error));
            }
        }
    }
    closed_ = true;
    return first_error_;
}

bool CsvOutputSet::failed() const noexcept {
    return static_cast<bool>(first_error_);
}

bool CsvOutputSet::closed() const noexcept {
    return closed_;
}

const OutputSetWriteError& CsvOutputSet::first_error() const noexcept {
    return first_error_;
}

CsvOutputSetMetrics CsvOutputSet::metrics() const noexcept {
    CsvOutputSetMetrics result;
    for (std::size_t index = 0; index < target_count_; ++index) {
        const TargetFiles& target = targets_[index];
        if (target.audit) {
            result.audit_rows_written +=
                target.audit->metrics().rows_written;
            result.audit_rows_buffered +=
                target.audit->buffered_rows();
        }
        if (target.order_book) {
            result.order_book_rows_written +=
                target.order_book->metrics().rows_written;
            result.order_book_rows_buffered +=
                target.order_book->buffered_rows();
        }
    }
    return result;
}

bool CsvOutputSet::valid_utc_date(
    const std::string_view value) noexcept {
    if (value.size() != 10U || value[4] != '-' || value[7] != '-') {
        return false;
    }
    constexpr std::array<std::size_t, 8U> digit_offsets{
        0U, 1U, 2U, 3U, 5U, 6U, 8U, 9U};
    if (!std::all_of(
            digit_offsets.begin(),
            digit_offsets.end(),
            [value](const std::size_t offset) {
                return is_ascii_digit(value[offset]);
            })) {
        return false;
    }
    const unsigned year = parse_four_digits(value);
    const unsigned month = parse_two_digits(value, 5U);
    const unsigned day = parse_two_digits(value, 8U);
    return year != 0U && day != 0U &&
           day <= days_in_month(year, month);
}

bool CsvOutputSet::valid_replay_stem(
    const std::string_view value) noexcept {
    if (value.empty() || value == "." || value == "..") {
        return false;
    }
    for (const char byte : value) {
        const auto unsigned_byte =
            static_cast<unsigned char>(byte);
        if (byte == '/' || byte == '\\' || byte == '\0' ||
            unsigned_byte < 0x20U || unsigned_byte == 0x7FU) {
            return false;
        }
    }
    return true;
}

std::string_view CsvOutputSet::venue_name(
    const PayloadVenue venue) noexcept {
    switch (venue) {
        case PayloadVenue::spot:
            return "spot";
        case PayloadVenue::usdm:
            return "usdm";
    }
    return {};
}

bool CsvOutputSet::prepare_output_directory(
    OutputSetOpenError& error) {
    namespace filesystem = std::filesystem;
    std::error_code native_error;
    const filesystem::path directory{output_directory_};
    const bool exists = filesystem::exists(directory, native_error);
    if (native_error) {
        error.code = OutputSetOpenErrorCode::file_system_failure;
        error.native_error = native_error.value();
        error.path = output_directory_;
        return false;
    }

    if (exists) {
        const bool is_directory =
            filesystem::is_directory(directory, native_error);
        if (native_error) {
            error.code =
                OutputSetOpenErrorCode::file_system_failure;
            error.native_error = native_error.value();
            error.path = output_directory_;
            return false;
        }
        if (!is_directory) {
            error.code =
                OutputSetOpenErrorCode::invalid_output_directory;
            error.path = output_directory_;
            return false;
        }
        const bool empty =
            filesystem::is_empty(directory, native_error);
        if (native_error) {
            error.code =
                OutputSetOpenErrorCode::file_system_failure;
            error.native_error = native_error.value();
            error.path = output_directory_;
            return false;
        }
        if (!empty) {
            error.code =
                OutputSetOpenErrorCode::output_directory_not_empty;
            error.path = output_directory_;
            return false;
        }
        return true;
    }

    if (!filesystem::create_directory(directory, native_error)) {
        error.code = native_error
            ? OutputSetOpenErrorCode::file_system_failure
            : OutputSetOpenErrorCode::invalid_output_directory;
        error.native_error =
            native_error ? native_error.value() : 0;
        error.path = output_directory_;
        return false;
    }
    directory_created_ = true;
    return true;
}

bool CsvOutputSet::open_files(OutputSetOpenError& error) {
    for (std::size_t index = 0; index < target_count_; ++index) {
        TargetFiles& target = targets_[index];
        if (mode_ == CsvOutputMode::live_capture) {
            FileSinkError file_error;
            target.audit = BufferedCsvFileSink::open_exclusive(
                target.audit_path,
                CsvFileKind::market_data,
                operations_,
                file_error);
            if (!target.audit) {
                error.code = OutputSetOpenErrorCode::file_sink_failure;
                error.target_index = index;
                error.file_kind = CsvFileKind::market_data;
                error.file_error = file_error;
                error.path = target.audit_path;
                if (file_error.code !=
                    FileSinkErrorCode::open_failed) {
                    const FileRemoveResult removal =
                        operations_->remove(target.audit_path);
                    if (!removal.success &&
                        removal.native_error != ENOENT) {
                        record_rollback_error(
                            error,
                            OutputRollbackOperation::remove_file,
                            index,
                            CsvFileKind::market_data,
                            {},
                            removal.native_error);
                    }
                }
                return false;
            }
            target.audit_created = true;
        }

        FileSinkError file_error;
        target.order_book = BufferedCsvFileSink::open_exclusive(
            target.order_book_path,
            CsvFileKind::order_book,
            operations_,
            file_error);
        if (!target.order_book) {
            error.code = OutputSetOpenErrorCode::file_sink_failure;
            error.target_index = index;
            error.file_kind = CsvFileKind::order_book;
            error.file_error = file_error;
            error.path = target.order_book_path;
            if (file_error.code != FileSinkErrorCode::open_failed) {
                const FileRemoveResult removal =
                    operations_->remove(target.order_book_path);
                if (!removal.success &&
                    removal.native_error != ENOENT) {
                    record_rollback_error(
                        error,
                        OutputRollbackOperation::remove_file,
                        index,
                        CsvFileKind::order_book,
                        {},
                        removal.native_error);
                }
            }
            return false;
        }
        target.order_book_created = true;
    }
    return true;
}

void CsvOutputSet::rollback_initialization(
    OutputSetOpenError& error) noexcept {
    for (std::size_t remaining = target_count_; remaining > 0U;
         --remaining) {
        const std::size_t index = remaining - 1U;
        TargetFiles& target = targets_[index];
        if (target.order_book) {
            const FileSinkError close_error =
                target.order_book->close();
            if (close_error) {
                record_rollback_error(
                    error,
                    OutputRollbackOperation::close_file,
                    index,
                    CsvFileKind::order_book,
                    close_error,
                    0);
            }
        }
        if (target.audit) {
            const FileSinkError close_error = target.audit->close();
            if (close_error) {
                record_rollback_error(
                    error,
                    OutputRollbackOperation::close_file,
                    index,
                    CsvFileKind::market_data,
                    close_error,
                    0);
            }
        }
        if (target.order_book_created) {
            const FileRemoveResult removal =
                operations_->remove(target.order_book_path);
            if (!removal.success && removal.native_error != ENOENT) {
                record_rollback_error(
                    error,
                    OutputRollbackOperation::remove_file,
                    index,
                    CsvFileKind::order_book,
                    {},
                    removal.native_error);
            }
            target.order_book_created = false;
        }
        if (target.audit_created) {
            const FileRemoveResult removal =
                operations_->remove(target.audit_path);
            if (!removal.success && removal.native_error != ENOENT) {
                record_rollback_error(
                    error,
                    OutputRollbackOperation::remove_file,
                    index,
                    CsvFileKind::market_data,
                    {},
                    removal.native_error);
            }
            target.audit_created = false;
        }
    }
    if (directory_created_) {
        if (::rmdir(output_directory_.c_str()) != 0 &&
            errno != ENOENT) {
            record_rollback_error(
                error,
                OutputRollbackOperation::remove_directory,
                kNoSymbolIndex,
                CsvFileKind::market_data,
                {},
                errno);
        }
        directory_created_ = false;
    }
    closed_ = true;
}

void CsvOutputSet::record_rollback_error(
    OutputSetOpenError& error,
    const OutputRollbackOperation operation,
    const std::size_t target_index,
    const CsvFileKind file_kind,
    const FileSinkError file_error,
    const int native_error) noexcept {
    if (!error.rollback_error) {
        error.rollback_error = OutputRollbackError{
            operation,
            target_index,
            file_kind,
            file_error,
            native_error};
    }
}

bool CsvOutputSet::valid_batch_contract(
    const EventRowBatch& batch) const noexcept {
    if (batch.has_audit_row == batch.audit_row.empty() ||
        batch.has_order_book_row == batch.order_book_row.empty()) {
        return false;
    }
    if (mode_ == CsvOutputMode::live_capture) {
        return !batch.has_order_book_row || batch.has_audit_row;
    }
    return !batch.has_audit_row;
}

OutputSetWriteError CsvOutputSet::latch_error(
    const OutputSetWriteErrorCode code,
    const std::size_t target_index,
    const CsvFileKind file_kind,
    const FileSinkError file_error) noexcept {
    if (!first_error_) {
        first_error_ = OutputSetWriteError{
            code, target_index, file_kind, file_error};
    }
    return first_error_;
}

}  // namespace hft
