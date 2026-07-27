#pragma once

#include "hft/spot_book.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace hft {

inline constexpr std::size_t kMaxCsvRecordBytes{
    3U * 1024U * 1024U};
inline constexpr std::size_t kDefaultCsvRecordCapacity{4096U};

inline constexpr std::string_view kMarketDataCsvHeader{
    "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,"
    "conn_epoch,conn_seq,symbol,payload_json\n"};
inline constexpr std::string_view kOrderBookCsvHeader{
    "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,"
    "bid4,bid_size0,bid_size1,bid_size2,bid_size3,bid_size4,"
    "ask0,ask1,ask2,ask3,ask4,ask_size0,ask_size1,ask_size2,"
    "ask_size3,ask_size4\n"};

enum class BookRowType : std::uint8_t {
    differential,
    partial_refresh,
};

enum class CsvFormatError : std::uint8_t {
    none,
    invalid_timestamp,
    invalid_sequence,
    invalid_symbol,
    invalid_instrument_id,
    invalid_enum,
    invalid_book,
    record_too_large,
    allocation_failure,
};

struct CsvTimestamp {
    std::uint64_t seconds{0};
    std::uint32_t nanoseconds{0};
};

struct MarketDataCsvRow {
    CsvTimestamp timestamp{};
    PayloadVenue venue{PayloadVenue::spot};
    SpotStreamKind stream_kind{SpotStreamKind::depth_diff};
    std::uint32_t shard_id{0};
    // connection_epoch starts at zero. connection_sequence is one-based
    // within (shard_id, connection_epoch) and zero is always invalid.
    std::uint64_t connection_epoch{0};
    std::uint64_t connection_sequence{0};
    // The routing layer supplies normalized [A-Z0-9]{1,32} bytes. The
    // payload layer supplies an already-minified, syntactically valid object.
    std::string_view symbol{};
    std::string_view payload_json{};
};

struct OrderBookCsvRow {
    CsvTimestamp timestamp{};
    std::uint64_t sequence_number{0};
    std::int32_t instrument_id{0};
    BookRowType row_type{BookRowType::differential};
    BookRowSide side{BookRowSide::neutral};
    // Views are consumed synchronously and are never retained by the buffer.
    BookSideView bids{};
    BookSideView asks{};
};

class CsvRecordBuffer {
public:
    explicit CsvRecordBuffer(
        std::size_t initial_capacity = kDefaultCsvRecordCapacity);

    [[nodiscard]] std::string_view view() const noexcept;
    [[nodiscard]] std::size_t size() const noexcept;
    [[nodiscard]] bool empty() const noexcept;
    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] std::size_t initial_capacity() const noexcept;
    void clear() noexcept;
    [[nodiscard]] bool release_excess_capacity() noexcept;

private:
    std::string storage_;
    std::size_t initial_capacity_{kDefaultCsvRecordCapacity};

    friend CsvFormatError format_market_data_csv_row(
        const MarketDataCsvRow& row,
        CsvRecordBuffer& output) noexcept;
    friend CsvFormatError format_order_book_csv_row(
        const OrderBookCsvRow& row,
        CsvRecordBuffer& output) noexcept;
};

[[nodiscard]] CsvFormatError format_market_data_csv_row(
    const MarketDataCsvRow& row,
    CsvRecordBuffer& output) noexcept;
[[nodiscard]] CsvFormatError format_order_book_csv_row(
    const OrderBookCsvRow& row,
    CsvRecordBuffer& output) noexcept;

[[nodiscard]] constexpr std::string_view to_string(
    const CsvFormatError error) noexcept {
    switch (error) {
        case CsvFormatError::none:
            return "none";
        case CsvFormatError::invalid_timestamp:
            return "invalid_timestamp";
        case CsvFormatError::invalid_sequence:
            return "invalid_sequence";
        case CsvFormatError::invalid_symbol:
            return "invalid_symbol";
        case CsvFormatError::invalid_instrument_id:
            return "invalid_instrument_id";
        case CsvFormatError::invalid_enum:
            return "invalid_enum";
        case CsvFormatError::invalid_book:
            return "invalid_book";
        case CsvFormatError::record_too_large:
            return "record_too_large";
        case CsvFormatError::allocation_failure:
            return "allocation_failure";
    }
    return "unknown";
}

}  // namespace hft
