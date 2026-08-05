#include "hft/market_data_replay.h"

#include <array>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <fcntl.h>
#include <new>
#include <optional>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>

namespace hft {
namespace {

constexpr std::array<ReplayColumn, 8U> kUnquotedColumns{
    ReplayColumn::recv_tsec,
    ReplayColumn::recv_tnsec,
    ReplayColumn::venue,
    ReplayColumn::stream_kind,
    ReplayColumn::shard_id,
    ReplayColumn::conn_epoch,
    ReplayColumn::conn_seq,
    ReplayColumn::symbol,
};

template <typename Integer>
[[nodiscard]] bool parse_integer(
    const std::string_view field,
    Integer& value) noexcept {
    if (field.empty()) {
        return false;
    }
    Integer parsed{0};
    const auto result = std::from_chars(
        field.data(), field.data() + field.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != field.data() + field.size()) {
        return false;
    }
    value = parsed;
    return true;
}

[[nodiscard]] bool parse_venue(
    const std::string_view field,
    PayloadVenue& venue) noexcept {
    if (field == "spot") {
        venue = PayloadVenue::spot;
        return true;
    }
    if (field == "usdm") {
        venue = PayloadVenue::usdm;
        return true;
    }
    return false;
}

[[nodiscard]] bool parse_stream_kind(
    const std::string_view field,
    SpotStreamKind& kind) noexcept {
    if (field == "depth_diff") {
        kind = SpotStreamKind::depth_diff;
        return true;
    }
    if (field == "depth5") {
        kind = SpotStreamKind::depth5;
        return true;
    }
    if (field == "trade") {
        kind = SpotStreamKind::trade;
        return true;
    }
    return false;
}

[[nodiscard]] bool forbidden_unquoted_byte(
    const char byte) noexcept {
    return byte == '"' || byte == '\r' || byte == '\n';
}

}  // namespace

ReplayRecord::ReplayRecord()
    : payload_bytes_{
          std::make_unique<char[]>(
              kMaxPayloadBytes + kJsonPaddingBytes)} {}

EventContext ReplayRecord::context() const noexcept {
    return EventContext{
        timestamp_,
        venue_,
        stream_kind_,
        shard_id_,
        connection_epoch_,
        connection_sequence_,
        symbol(),
    };
}

PaddedJsonView ReplayRecord::payload() const noexcept {
    return PaddedJsonView{
        payload_bytes_.get(),
        payload_size_,
        kMaxPayloadBytes + kJsonPaddingBytes};
}

std::string_view ReplayRecord::symbol() const noexcept {
    return std::string_view{symbol_bytes_.data(), symbol_size_};
}

MarketDataReplayReader::MarketDataReplayReader(
    std::string path,
    const int descriptor)
    : path_{std::move(path)},
      descriptor_{descriptor},
      read_buffer_{
          std::make_unique<char[]>(kReplayReadBufferBytes)},
      record_buffer_{
          std::make_unique<char[]>(kMaxCsvRecordBytes)} {}

std::unique_ptr<MarketDataReplayReader>
MarketDataReplayReader::open(
    std::string path,
    ReplayReadError& error) noexcept {
    error = {};
    if (path.empty()) {
        error.code = ReplayReadErrorCode::invalid_path;
        error.column = ReplayColumn::header;
        error.logical_record_number = 1U;
        return nullptr;
    }

    const int descriptor =
        ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (descriptor < 0) {
        error.code = ReplayReadErrorCode::open_failed;
        error.column = ReplayColumn::header;
        error.logical_record_number = 1U;
        error.native_error = errno;
        return nullptr;
    }

    std::unique_ptr<MarketDataReplayReader> reader;
    try {
        reader.reset(new MarketDataReplayReader{
            path, descriptor});
    } catch (const std::bad_alloc&) {
        static_cast<void>(::close(descriptor));
        error.code = ReplayReadErrorCode::allocation_failure;
        error.column = ReplayColumn::header;
        error.logical_record_number = 1U;
        return nullptr;
    }

    std::string_view header;
    ReplayReadError header_error;
    const LogicalReadStatus status =
        reader->read_logical_record(header, header_error);
    if (status != LogicalReadStatus::record ||
        header != kMarketDataCsvHeader) {
        if (status == LogicalReadStatus::error) {
            error = header_error;
            error.column = ReplayColumn::header;
        } else {
            error.code = ReplayReadErrorCode::invalid_header;
            error.column = ReplayColumn::header;
            error.logical_record_number = 1U;
        }
        return nullptr;
    }
    reader->logical_record_number_ = 1U;
    return reader;
}

MarketDataReplayReader::~MarketDataReplayReader() noexcept {
    if (descriptor_ >= 0) {
        static_cast<void>(::close(descriptor_));
    }
}

ReplayReadResult MarketDataReplayReader::next() noexcept {
    if (first_error_) {
        return ReplayReadResult{
            ReplayReadStatus::error, first_error_};
    }
    if (end_of_file_) {
        return ReplayReadResult{
            ReplayReadStatus::end_of_file, {}};
    }

    std::string_view logical_record;
    ReplayReadError read_error;
    const LogicalReadStatus status =
        read_logical_record(logical_record, read_error);
    if (status == LogicalReadStatus::end_of_file) {
        end_of_file_ = true;
        return ReplayReadResult{
            ReplayReadStatus::end_of_file, {}};
    }
    if (status == LogicalReadStatus::error) {
        return latch(read_error);
    }

    const ReplayReadError decode_error =
        decode_record(logical_record);
    if (decode_error) {
        return latch(decode_error);
    }
    const ReplayReadError sequence_error =
        validate_file_sequence();
    if (sequence_error) {
        return latch(sequence_error);
    }
    ++logical_record_number_;
    return ReplayReadResult{ReplayReadStatus::record, {}};
}

const ReplayRecord& MarketDataReplayReader::record() const noexcept {
    return record_;
}

std::uint64_t
MarketDataReplayReader::logical_record_number() const noexcept {
    return logical_record_number_;
}

const std::string& MarketDataReplayReader::path() const noexcept {
    return path_;
}

const ReplayReadError&
MarketDataReplayReader::first_error() const noexcept {
    return first_error_;
}

MarketDataReplayReader::ByteReadStatus
MarketDataReplayReader::read_byte(
    char& value,
    int& native_error) noexcept {
    if (read_begin_ == read_size_) {
        while (true) {
            const ssize_t result = ::read(
                descriptor_,
                read_buffer_.get(),
                kReplayReadBufferBytes);
            if (result > 0) {
                read_begin_ = 0U;
                read_size_ =
                    static_cast<std::size_t>(result);
                break;
            }
            if (result == 0) {
                return ByteReadStatus::end_of_file;
            }
            if (errno != EINTR) {
                native_error = errno;
                return ByteReadStatus::error;
            }
        }
    }

    value = read_buffer_[read_begin_];
    ++read_begin_;
    return ByteReadStatus::byte;
}

MarketDataReplayReader::LogicalReadStatus
MarketDataReplayReader::read_logical_record(
    std::string_view& record,
    ReplayReadError& error) noexcept {
    record = {};
    error = {};
    record_size_ = 0U;
    CsvFrameState state = CsvFrameState::field_start;

    while (true) {
        char byte{0};
        int native_error{0};
        const ByteReadStatus read_status =
            read_byte(byte, native_error);
        if (read_status == ByteReadStatus::error) {
            error = make_error(
                ReplayReadErrorCode::read_failed,
                ReplayColumn::none,
                native_error);
            return LogicalReadStatus::error;
        }
        if (read_status == ByteReadStatus::end_of_file) {
            if (record_size_ == 0U) {
                return LogicalReadStatus::end_of_file;
            }
            error = make_error(
                ReplayReadErrorCode::truncated_record,
                ReplayColumn::none);
            return LogicalReadStatus::error;
        }
        if (record_size_ == kMaxCsvRecordBytes) {
            error = make_error(
                ReplayReadErrorCode::record_too_large,
                ReplayColumn::none);
            return LogicalReadStatus::error;
        }
        record_buffer_[record_size_] = byte;
        ++record_size_;

        switch (state) {
            case CsvFrameState::field_start:
                if (byte == '"') {
                    state = CsvFrameState::quoted;
                } else if (byte == ',') {
                    state = CsvFrameState::field_start;
                } else if (byte == '\n') {
                    record = std::string_view{
                        record_buffer_.get(), record_size_};
                    return LogicalReadStatus::record;
                } else {
                    state = CsvFrameState::unquoted;
                }
                break;
            case CsvFrameState::unquoted:
                if (byte == '"') {
                    error = make_error(
                        ReplayReadErrorCode::invalid_csv,
                        ReplayColumn::none);
                    return LogicalReadStatus::error;
                }
                if (byte == ',') {
                    state = CsvFrameState::field_start;
                } else if (byte == '\n') {
                    record = std::string_view{
                        record_buffer_.get(), record_size_};
                    return LogicalReadStatus::record;
                }
                break;
            case CsvFrameState::quoted:
                if (byte == '"') {
                    state = CsvFrameState::quote_closed;
                }
                break;
            case CsvFrameState::quote_closed:
                if (byte == '"') {
                    state = CsvFrameState::quoted;
                } else if (byte == ',') {
                    state = CsvFrameState::field_start;
                } else if (byte == '\n') {
                    record = std::string_view{
                        record_buffer_.get(), record_size_};
                    return LogicalReadStatus::record;
                } else {
                    error = make_error(
                        ReplayReadErrorCode::invalid_csv,
                        ReplayColumn::none);
                    return LogicalReadStatus::error;
                }
                break;
        }
    }
}

ReplayReadError MarketDataReplayReader::decode_record(
    const std::string_view logical_record) noexcept {
    if (logical_record.empty() ||
        logical_record.back() != '\n') {
        return make_error(
            ReplayReadErrorCode::truncated_record,
            ReplayColumn::none);
    }
    const std::string_view row =
        logical_record.substr(0U, logical_record.size() - 1U);

    std::array<std::string_view, 8U> fields{};
    std::size_t begin{0};
    for (std::size_t index = 0; index < fields.size(); ++index) {
        std::size_t comma = begin;
        while (comma < row.size() && row[comma] != ',') {
            if (forbidden_unquoted_byte(row[comma])) {
                return make_error(
                    ReplayReadErrorCode::invalid_csv,
                    kUnquotedColumns[index]);
            }
            ++comma;
        }
        if (comma == row.size()) {
            return make_error(
                ReplayReadErrorCode::wrong_column_count,
                kUnquotedColumns[index]);
        }
        fields[index] = row.substr(begin, comma - begin);
        begin = comma + 1U;
    }

    const std::string_view encoded_payload = row.substr(begin);
    if (encoded_payload.size() < 2U ||
        encoded_payload.front() != '"' ||
        encoded_payload.back() != '"') {
        return make_error(
            ReplayReadErrorCode::invalid_csv,
            ReplayColumn::payload_json);
    }

    std::int64_t seconds{0};
    if (!parse_integer(fields[0], seconds)) {
        return make_error(
            ReplayReadErrorCode::invalid_integer,
            ReplayColumn::recv_tsec);
    }
    std::int32_t nanoseconds{0};
    if (!parse_integer(fields[1], nanoseconds)) {
        return make_error(
            ReplayReadErrorCode::invalid_integer,
            ReplayColumn::recv_tnsec);
    }
    if (nanoseconds < 0 || nanoseconds >= 1'000'000'000) {
        return make_error(
            ReplayReadErrorCode::invalid_range,
            ReplayColumn::recv_tnsec);
    }

    PayloadVenue venue{PayloadVenue::spot};
    if (!parse_venue(fields[2], venue)) {
        return make_error(
            ReplayReadErrorCode::invalid_enum,
            ReplayColumn::venue);
    }
    SpotStreamKind stream_kind{SpotStreamKind::depth_diff};
    if (!parse_stream_kind(fields[3], stream_kind)) {
        return make_error(
            ReplayReadErrorCode::invalid_enum,
            ReplayColumn::stream_kind);
    }

    std::uint32_t shard_id{0};
    if (!parse_integer(fields[4], shard_id)) {
        return make_error(
            ReplayReadErrorCode::invalid_integer,
            ReplayColumn::shard_id);
    }
    if (shard_id != 0U) {
        return make_error(
            ReplayReadErrorCode::invalid_range,
            ReplayColumn::shard_id);
    }
    std::uint64_t connection_epoch{0};
    if (!parse_integer(fields[5], connection_epoch)) {
        return make_error(
            ReplayReadErrorCode::invalid_integer,
            ReplayColumn::conn_epoch);
    }
    std::uint64_t connection_sequence{0};
    if (!parse_integer(
            fields[6], connection_sequence)) {
        return make_error(
            ReplayReadErrorCode::invalid_integer,
            ReplayColumn::conn_seq);
    }
    if (connection_sequence == 0U) {
        return make_error(
            ReplayReadErrorCode::invalid_range,
            ReplayColumn::conn_seq);
    }
    if (!is_normalized_symbol(fields[7])) {
        return make_error(
            ReplayReadErrorCode::invalid_symbol,
            ReplayColumn::symbol);
    }

    std::size_t decoded_size{0};
    std::size_t offset{1U};
    while (offset + 1U < encoded_payload.size()) {
        const char byte = encoded_payload[offset];
        if (byte == '"') {
            if (offset + 1U >= encoded_payload.size() - 1U ||
                encoded_payload[offset + 1U] != '"') {
                return make_error(
                    ReplayReadErrorCode::invalid_csv,
                    ReplayColumn::payload_json);
            }
            if (decoded_size == kMaxPayloadBytes) {
                return make_error(
                    ReplayReadErrorCode::payload_too_large,
                    ReplayColumn::payload_json);
            }
            record_.payload_bytes_[decoded_size] = '"';
            ++decoded_size;
            offset += 2U;
            continue;
        }
        if (decoded_size == kMaxPayloadBytes) {
            return make_error(
                ReplayReadErrorCode::payload_too_large,
                ReplayColumn::payload_json);
        }
        record_.payload_bytes_[decoded_size] = byte;
        ++decoded_size;
        ++offset;
    }

    record_.timestamp_ = CsvTimestamp{seconds, nanoseconds};
    record_.venue_ = venue;
    record_.stream_kind_ = stream_kind;
    record_.shard_id_ = shard_id;
    record_.connection_epoch_ = connection_epoch;
    record_.connection_sequence_ = connection_sequence;
    std::memcpy(
        record_.symbol_bytes_.data(),
        fields[7].data(),
        fields[7].size());
    record_.symbol_size_ = fields[7].size();
    record_.payload_size_ = decoded_size;
    std::memset(
        record_.payload_bytes_.get() + decoded_size,
        0,
        kJsonPaddingBytes);
    return {};
}

ReplayReadError
MarketDataReplayReader::validate_file_sequence() noexcept {
    if (!has_file_identity_) {
        file_venue_ = record_.venue_;
        std::memcpy(
            file_symbol_.data(),
            record_.symbol_bytes_.data(),
            record_.symbol_size_);
        file_symbol_size_ = record_.symbol_size_;
        last_connection_epoch_ = record_.connection_epoch_;
        last_connection_sequence_ =
            record_.connection_sequence_;
        has_file_identity_ = true;
        return {};
    }

    if (record_.venue_ != file_venue_) {
        return make_error(
            ReplayReadErrorCode::changing_venue,
            ReplayColumn::venue);
    }
    if (record_.symbol_size_ != file_symbol_size_ ||
        std::memcmp(
            record_.symbol_bytes_.data(),
            file_symbol_.data(),
            file_symbol_size_) != 0) {
        return make_error(
            ReplayReadErrorCode::changing_symbol,
            ReplayColumn::symbol);
    }
    if (record_.connection_epoch_ < last_connection_epoch_) {
        return make_error(
            ReplayReadErrorCode::decreasing_epoch,
            ReplayColumn::conn_epoch);
    }
    if (record_.connection_epoch_ == last_connection_epoch_ &&
        record_.connection_sequence_ <=
            last_connection_sequence_) {
        return make_error(
            ReplayReadErrorCode::
                non_increasing_connection_sequence,
            ReplayColumn::conn_seq);
    }

    last_connection_epoch_ = record_.connection_epoch_;
    last_connection_sequence_ = record_.connection_sequence_;
    return {};
}

ReplayReadError MarketDataReplayReader::make_error(
    const ReplayReadErrorCode code,
    const ReplayColumn column,
    const int native_error) const noexcept {
    return ReplayReadError{
        code,
        column,
        logical_record_number_ + 1U,
        native_error};
}

ReplayReadResult MarketDataReplayReader::latch(
    const ReplayReadError error) noexcept {
    if (!first_error_) {
        first_error_ = error;
    }
    return ReplayReadResult{
        ReplayReadStatus::error, first_error_};
}

namespace {

ReplayFileResult replay_market_data_file_impl(
    std::string input_path,
    CsvOutputSet& output,
    const std::size_t target_index) {
    ReplayFileResult result;
    ReplayReadError open_error;
    std::unique_ptr<MarketDataReplayReader> reader =
        MarketDataReplayReader::open(
            std::move(input_path), open_error);
    if (!reader) {
        result.error = ReplayFileErrorCode::input_error;
        result.input_error = open_error;
        result.logical_record_number =
            open_error.logical_record_number;
        return result;
    }

    EventProcessor processor;
    EventRowBatch batch;
    std::optional<SymbolState> state;
    while (true) {
        const ReplayReadResult read = reader->next();
        if (read.status == ReplayReadStatus::end_of_file) {
            if (!state) {
                result.error = ReplayFileErrorCode::empty_input;
                result.logical_record_number = 2U;
            }
            return result;
        }
        if (read.status == ReplayReadStatus::error) {
            result.error = ReplayFileErrorCode::input_error;
            result.input_error = read.error;
            result.logical_record_number =
                read.error.logical_record_number;
            return result;
        }

        ++result.rows_read;
        result.logical_record_number =
            reader->logical_record_number();
        const ReplayRecord& record = reader->record();
        if (!state) {
            if (output.symbol(target_index) != record.symbol()) {
                result.error =
                    ReplayFileErrorCode::output_target_mismatch;
                return result;
            }
            state = SymbolState::create(
                record.context().venue, record.symbol());
            if (!state) {
                result.error =
                    ReplayFileErrorCode::state_creation_failed;
                return result;
            }
        }

        const EventProcessResult processed = processor.process(
            *state,
            record.context(),
            record.payload(),
            EventOutputMode::replay,
            batch);
        result.process_error = processed;
        if (!processed.success()) {
            result.error =
                ReplayFileErrorCode::event_process_failed;
            return result;
        }
        if (processed.status ==
            EventProcessStatus::pre_audit_rejected) {
            result.error = ReplayFileErrorCode::payload_rejected;
            return result;
        }

        ++result.rows_processed;
        if (batch.has_order_book_row) {
            ++result.order_book_rows;
        }
        const OutputSetWriteError write_error =
            output.write_batch(target_index, batch);
        if (write_error) {
            result.error =
                ReplayFileErrorCode::output_write_failed;
            result.output_error = write_error;
            return result;
        }
    }
}

}  // namespace

ReplayFileResult replay_market_data_file(
    std::string input_path,
    CsvOutputSet& output,
    const std::size_t target_index) noexcept {
    try {
        return replay_market_data_file_impl(
            std::move(input_path), output, target_index);
    } catch (const std::bad_alloc&) {
        ReplayFileResult result;
        result.error = ReplayFileErrorCode::allocation_failure;
        return result;
    }
}

}  // namespace hft
