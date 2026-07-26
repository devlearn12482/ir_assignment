#include "hft/csv_formatter.h"
#include "hft/symbol_identity.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>

namespace hft {
namespace {

inline constexpr std::uint32_t kNanosecondsPerSecond{
    1'000'000'000U};
[[nodiscard]] bool append_bytes(
    std::string& output,
    const std::string_view bytes) {
    if (output.size() > kMaxCsvRecordBytes ||
        bytes.size() > kMaxCsvRecordBytes - output.size()) {
        return false;
    }
    if (bytes.empty()) {
        return true;
    }
    output.append(bytes.data(), bytes.size());
    return true;
}

[[nodiscard]] bool append_byte(
    std::string& output,
    const char byte) {
    if (output.size() == kMaxCsvRecordBytes) {
        return false;
    }
    output.push_back(byte);
    return true;
}

template <typename Integer>
[[nodiscard]] bool append_integer(
    std::string& output,
    const Integer value) {
    static_assert(std::is_integral<Integer>::value);
    std::array<char, 32U> scratch{};
    const auto result = std::to_chars(
        scratch.data(), scratch.data() + scratch.size(), value);
    if (result.ec != std::errc{}) {
        return false;
    }
    return append_bytes(
        output,
        std::string_view{
            scratch.data(),
            static_cast<std::size_t>(result.ptr - scratch.data())});
}

[[nodiscard]] bool append_comma(
    std::string& output) {
    return append_byte(output, ',');
}

[[nodiscard]] bool append_quoted_field(
    std::string& output,
    const std::string_view field) {
    if (!append_byte(output, '"')) {
        return false;
    }
    for (const char byte : field) {
        if (byte == '"') {
            if (!append_bytes(output, "\"\"")) {
                return false;
            }
        } else if (!append_byte(output, byte)) {
            return false;
        }
    }
    return append_byte(output, '"');
}

[[nodiscard]] bool valid_timestamp(
    const CsvTimestamp timestamp) noexcept {
    return timestamp.nanoseconds < kNanosecondsPerSecond;
}

[[nodiscard]] std::string_view venue_text(
    const PayloadVenue venue) noexcept {
    switch (venue) {
        case PayloadVenue::spot:
            return "spot";
        case PayloadVenue::usdm:
            return "usdm";
    }
    return {};
}

[[nodiscard]] std::string_view stream_kind_text(
    const SpotStreamKind kind) noexcept {
    switch (kind) {
        case SpotStreamKind::depth_diff:
            return "depth_diff";
        case SpotStreamKind::depth5:
            return "depth5";
        case SpotStreamKind::trade:
            return "trade";
    }
    return {};
}

[[nodiscard]] char row_type_text(
    const BookRowType row_type) noexcept {
    switch (row_type) {
        case BookRowType::differential:
            return 'D';
        case BookRowType::partial_refresh:
            return 'P';
    }
    return '\0';
}

[[nodiscard]] char side_text(
    const BookRowSide side) noexcept {
    switch (side) {
        case BookRowSide::bid:
            return 'B';
        case BookRowSide::ask:
            return 'S';
        case BookRowSide::neutral:
            return 'N';
    }
    return '\0';
}

[[nodiscard]] bool valid_side(
    const BookSideView side,
    const BookSide book_side) noexcept {
    if (side.size > kVisibleBookDepth ||
        (side.size != 0U && side.data == nullptr)) {
        return false;
    }
    for (std::size_t index = 0; index < side.size; ++index) {
        const BookLevel level = side[index];
        if (level.price <= 0 || level.quantity <= 0) {
            return false;
        }
        if (index == 0U) {
            continue;
        }
        const std::int64_t previous_price =
            side[index - 1U].price;
        if (book_side == BookSide::bid
                ? previous_price <= level.price
                : previous_price >= level.price) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool valid_book(
    const BookSideView bids,
    const BookSideView asks) noexcept {
    if (!valid_side(bids, BookSide::bid) ||
        !valid_side(asks, BookSide::ask)) {
        return false;
    }
    return bids.size == 0U || asks.size == 0U ||
           bids[0U].price < asks[0U].price;
}

[[nodiscard]] bool append_level_component(
    std::string& output,
    const BookSideView side,
    const std::size_t index,
    const bool quantity) {
    if (!append_comma(output)) {
        return false;
    }
    if (index >= side.size) {
        return append_byte(output, '0');
    }
    return append_integer(
        output,
        quantity ? side[index].quantity : side[index].price);
}

[[nodiscard]] bool append_book_side(
    std::string& output,
    const BookSideView side,
    const bool quantity) {
    for (std::size_t index = 0; index < kVisibleBookDepth; ++index) {
        if (!append_level_component(
                output, side, index, quantity)) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] CsvFormatError fail(
    CsvRecordBuffer& output,
    const CsvFormatError error) noexcept {
    output.clear();
    return error;
}

}  // namespace

CsvRecordBuffer::CsvRecordBuffer(
    const std::size_t initial_capacity) {
    storage_.reserve(
        std::min(initial_capacity, kMaxCsvRecordBytes));
}

std::string_view CsvRecordBuffer::view() const noexcept {
    return storage_;
}

std::size_t CsvRecordBuffer::size() const noexcept {
    return storage_.size();
}

bool CsvRecordBuffer::empty() const noexcept {
    return storage_.empty();
}

std::size_t CsvRecordBuffer::capacity() const noexcept {
    return storage_.capacity();
}

void CsvRecordBuffer::clear() noexcept {
    storage_.clear();
}

CsvFormatError format_market_data_csv_row(
    const MarketDataCsvRow& row,
    CsvRecordBuffer& output) noexcept {
    output.clear();
    if (!valid_timestamp(row.timestamp)) {
        return CsvFormatError::invalid_timestamp;
    }
    if (row.connection_sequence == 0U) {
        return CsvFormatError::invalid_sequence;
    }
    if (!is_normalized_symbol(row.symbol)) {
        return CsvFormatError::invalid_symbol;
    }
    const std::string_view venue = venue_text(row.venue);
    const std::string_view stream_kind =
        stream_kind_text(row.stream_kind);
    if (venue.empty() || stream_kind.empty()) {
        return CsvFormatError::invalid_enum;
    }

    try {
        const bool complete =
            append_integer(output.storage_, row.timestamp.seconds) &&
            append_comma(output.storage_) &&
            append_integer(
                output.storage_, row.timestamp.nanoseconds) &&
            append_comma(output.storage_) &&
            append_bytes(output.storage_, venue) &&
            append_comma(output.storage_) &&
            append_bytes(output.storage_, stream_kind) &&
            append_comma(output.storage_) &&
            append_integer(output.storage_, row.shard_id) &&
            append_comma(output.storage_) &&
            append_integer(
                output.storage_, row.connection_epoch) &&
            append_comma(output.storage_) &&
            append_integer(
                output.storage_, row.connection_sequence) &&
            append_comma(output.storage_) &&
            append_bytes(output.storage_, row.symbol) &&
            append_comma(output.storage_) &&
            append_quoted_field(
                output.storage_, row.payload_json) &&
            append_byte(output.storage_, '\n');
        if (!complete) {
            return fail(output, CsvFormatError::record_too_large);
        }
    } catch (const std::bad_alloc&) {
        return fail(output, CsvFormatError::allocation_failure);
    }
    return CsvFormatError::none;
}

CsvFormatError format_order_book_csv_row(
    const OrderBookCsvRow& row,
    CsvRecordBuffer& output) noexcept {
    output.clear();
    if (!valid_timestamp(row.timestamp)) {
        return CsvFormatError::invalid_timestamp;
    }
    if (row.sequence_number == 0U) {
        return CsvFormatError::invalid_sequence;
    }
    if (row.instrument_id <= 0) {
        return CsvFormatError::invalid_instrument_id;
    }
    const char row_type = row_type_text(row.row_type);
    const char side = side_text(row.side);
    if (row_type == '\0' || side == '\0') {
        return CsvFormatError::invalid_enum;
    }
    if (!valid_book(row.bids, row.asks)) {
        return CsvFormatError::invalid_book;
    }

    try {
        const bool complete =
            append_integer(output.storage_, row.timestamp.seconds) &&
            append_comma(output.storage_) &&
            append_integer(
                output.storage_, row.timestamp.nanoseconds) &&
            append_comma(output.storage_) &&
            append_integer(
                output.storage_, row.sequence_number) &&
            append_comma(output.storage_) &&
            append_integer(output.storage_, row.instrument_id) &&
            append_comma(output.storage_) &&
            append_byte(output.storage_, row_type) &&
            append_comma(output.storage_) &&
            append_byte(output.storage_, side) &&
            append_book_side(output.storage_, row.bids, false) &&
            append_book_side(output.storage_, row.bids, true) &&
            append_book_side(output.storage_, row.asks, false) &&
            append_book_side(output.storage_, row.asks, true) &&
            append_byte(output.storage_, '\n');
        if (!complete) {
            return fail(output, CsvFormatError::record_too_large);
        }
    } catch (const std::bad_alloc&) {
        return fail(output, CsvFormatError::allocation_failure);
    }
    return CsvFormatError::none;
}

}  // namespace hft
