#pragma once

#include "hft/csv_output_set.h"
#include "hft/event_processor.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace hft {

inline constexpr std::size_t kReplayReadBufferBytes{64U * 1024U};

enum class ReplayColumn : std::uint8_t {
    none,
    header,
    recv_tsec,
    recv_tnsec,
    venue,
    stream_kind,
    shard_id,
    conn_epoch,
    conn_seq,
    symbol,
    payload_json,
};

enum class ReplayReadErrorCode : std::uint8_t {
    none,
    invalid_path,
    open_failed,
    allocation_failure,
    read_failed,
    invalid_header,
    record_too_large,
    truncated_record,
    invalid_csv,
    wrong_column_count,
    invalid_integer,
    invalid_range,
    invalid_enum,
    invalid_symbol,
    payload_too_large,
    changing_symbol,
    changing_venue,
    decreasing_epoch,
    non_increasing_connection_sequence,
};

struct ReplayReadError {
    ReplayReadErrorCode code{ReplayReadErrorCode::none};
    ReplayColumn column{ReplayColumn::none};
    std::uint64_t logical_record_number{0};
    int native_error{0};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != ReplayReadErrorCode::none;
    }
};

enum class ReplayReadStatus : std::uint8_t {
    record,
    end_of_file,
    error,
};

struct ReplayReadResult {
    ReplayReadStatus status{ReplayReadStatus::error};
    ReplayReadError error{};
};

class ReplayRecord {
public:
    ReplayRecord(const ReplayRecord&) = delete;
    ReplayRecord& operator=(const ReplayRecord&) = delete;
    ReplayRecord(ReplayRecord&&) = delete;
    ReplayRecord& operator=(ReplayRecord&&) = delete;

    [[nodiscard]] EventContext context() const noexcept;
    [[nodiscard]] PaddedJsonView payload() const noexcept;
    [[nodiscard]] std::string_view symbol() const noexcept;

private:
    ReplayRecord();

    CsvTimestamp timestamp_{};
    PayloadVenue venue_{PayloadVenue::spot};
    SpotStreamKind stream_kind_{SpotStreamKind::depth_diff};
    std::uint32_t shard_id_{0};
    std::uint64_t connection_epoch_{0};
    std::uint64_t connection_sequence_{0};
    std::array<char, kMaxNormalizedSymbolBytes> symbol_bytes_{};
    std::size_t symbol_size_{0};
    std::unique_ptr<char[]> payload_bytes_{};
    std::size_t payload_size_{0};

    friend class MarketDataReplayReader;
};

class MarketDataReplayReader {
public:
    [[nodiscard]] static std::unique_ptr<MarketDataReplayReader> open(
        std::string path,
        ReplayReadError& error) noexcept;

    MarketDataReplayReader(const MarketDataReplayReader&) = delete;
    MarketDataReplayReader& operator=(
        const MarketDataReplayReader&) = delete;
    MarketDataReplayReader(MarketDataReplayReader&&) = delete;
    MarketDataReplayReader& operator=(
        MarketDataReplayReader&&) = delete;

    ~MarketDataReplayReader() noexcept;

    [[nodiscard]] ReplayReadResult next() noexcept;
    [[nodiscard]] const ReplayRecord& record() const noexcept;
    [[nodiscard]] std::uint64_t logical_record_number() const noexcept;
    [[nodiscard]] const std::string& path() const noexcept;
    [[nodiscard]] const ReplayReadError& first_error() const noexcept;

private:
    enum class ByteReadStatus : std::uint8_t {
        byte,
        end_of_file,
        error,
    };

    enum class LogicalReadStatus : std::uint8_t {
        record,
        end_of_file,
        error,
    };

    enum class CsvFrameState : std::uint8_t {
        field_start,
        unquoted,
        quoted,
        quote_closed,
    };

    MarketDataReplayReader(std::string path, int descriptor);

    [[nodiscard]] ByteReadStatus read_byte(
        char& value,
        int& native_error) noexcept;
    [[nodiscard]] LogicalReadStatus read_logical_record(
        std::string_view& record,
        ReplayReadError& error) noexcept;
    [[nodiscard]] ReplayReadError decode_record(
        std::string_view record) noexcept;
    [[nodiscard]] ReplayReadError validate_file_sequence() noexcept;
    [[nodiscard]] ReplayReadError make_error(
        ReplayReadErrorCode code,
        ReplayColumn column,
        int native_error = 0) const noexcept;
    [[nodiscard]] ReplayReadResult latch(
        ReplayReadError error) noexcept;

    std::string path_{};
    int descriptor_{-1};
    std::unique_ptr<char[]> read_buffer_{};
    std::size_t read_begin_{0};
    std::size_t read_size_{0};
    std::unique_ptr<char[]> record_buffer_{};
    std::size_t record_size_{0};
    ReplayRecord record_{};
    std::uint64_t logical_record_number_{0};
    bool end_of_file_{false};
    bool has_file_identity_{false};
    PayloadVenue file_venue_{PayloadVenue::spot};
    std::array<char, kMaxNormalizedSymbolBytes> file_symbol_{};
    std::size_t file_symbol_size_{0};
    std::uint64_t last_connection_epoch_{0};
    std::uint64_t last_connection_sequence_{0};
    ReplayReadError first_error_{};
};

enum class ReplayFileErrorCode : std::uint8_t {
    none,
    allocation_failure,
    input_error,
    empty_input,
    output_target_mismatch,
    state_creation_failed,
    event_process_failed,
    payload_rejected,
    output_write_failed,
};

struct ReplayFileResult {
    ReplayFileErrorCode error{ReplayFileErrorCode::none};
    ReplayReadError input_error{};
    EventProcessResult process_error{};
    OutputSetWriteError output_error{};
    std::uint64_t logical_record_number{0};
    std::uint64_t rows_read{0};
    std::uint64_t rows_processed{0};
    std::uint64_t order_book_rows{0};

    [[nodiscard]] bool success() const noexcept {
        return error == ReplayFileErrorCode::none;
    }
};

[[nodiscard]] ReplayFileResult replay_market_data_file(
    std::string input_path,
    CsvOutputSet& output,
    std::size_t target_index) noexcept;

[[nodiscard]] constexpr std::string_view to_string(
    ReplayReadErrorCode code) noexcept {
    switch (code) {
        case ReplayReadErrorCode::none:
            return "none";
        case ReplayReadErrorCode::invalid_path:
            return "invalid_path";
        case ReplayReadErrorCode::open_failed:
            return "open_failed";
        case ReplayReadErrorCode::allocation_failure:
            return "allocation_failure";
        case ReplayReadErrorCode::read_failed:
            return "read_failed";
        case ReplayReadErrorCode::invalid_header:
            return "invalid_header";
        case ReplayReadErrorCode::record_too_large:
            return "record_too_large";
        case ReplayReadErrorCode::truncated_record:
            return "truncated_record";
        case ReplayReadErrorCode::invalid_csv:
            return "invalid_csv";
        case ReplayReadErrorCode::wrong_column_count:
            return "wrong_column_count";
        case ReplayReadErrorCode::invalid_integer:
            return "invalid_integer";
        case ReplayReadErrorCode::invalid_range:
            return "invalid_range";
        case ReplayReadErrorCode::invalid_enum:
            return "invalid_enum";
        case ReplayReadErrorCode::invalid_symbol:
            return "invalid_symbol";
        case ReplayReadErrorCode::payload_too_large:
            return "payload_too_large";
        case ReplayReadErrorCode::changing_symbol:
            return "changing_symbol";
        case ReplayReadErrorCode::changing_venue:
            return "changing_venue";
        case ReplayReadErrorCode::decreasing_epoch:
            return "decreasing_epoch";
        case ReplayReadErrorCode::non_increasing_connection_sequence:
            return "non_increasing_connection_sequence";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    ReplayColumn column) noexcept {
    switch (column) {
        case ReplayColumn::none:
            return "none";
        case ReplayColumn::header:
            return "header";
        case ReplayColumn::recv_tsec:
            return "recv_tsec";
        case ReplayColumn::recv_tnsec:
            return "recv_tnsec";
        case ReplayColumn::venue:
            return "venue";
        case ReplayColumn::stream_kind:
            return "stream_kind";
        case ReplayColumn::shard_id:
            return "shard_id";
        case ReplayColumn::conn_epoch:
            return "conn_epoch";
        case ReplayColumn::conn_seq:
            return "conn_seq";
        case ReplayColumn::symbol:
            return "symbol";
        case ReplayColumn::payload_json:
            return "payload_json";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    ReplayFileErrorCode code) noexcept {
    switch (code) {
        case ReplayFileErrorCode::none:
            return "none";
        case ReplayFileErrorCode::allocation_failure:
            return "allocation_failure";
        case ReplayFileErrorCode::input_error:
            return "input_error";
        case ReplayFileErrorCode::empty_input:
            return "empty_input";
        case ReplayFileErrorCode::output_target_mismatch:
            return "output_target_mismatch";
        case ReplayFileErrorCode::state_creation_failed:
            return "state_creation_failed";
        case ReplayFileErrorCode::event_process_failed:
            return "event_process_failed";
        case ReplayFileErrorCode::payload_rejected:
            return "payload_rejected";
        case ReplayFileErrorCode::output_write_failed:
            return "output_write_failed";
    }
    return "unknown";
}

}  // namespace hft
