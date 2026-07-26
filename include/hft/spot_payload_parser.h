#pragma once

#include "hft/fixed_point.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace hft {

inline constexpr std::size_t kMaxPayloadBytes{1024U * 1024U};
inline constexpr std::size_t kJsonPaddingBytes{64U};
// Number of simultaneously open JSON arrays/objects, including the root.
inline constexpr std::size_t kMaxJsonNestingDepth{64U};
inline constexpr std::size_t kMaxDepthUpdates{16'384U};
inline constexpr std::size_t kMaxPartialDepthLevelsPerSide{5U};

enum class SpotStreamKind : std::uint8_t {
    depth_diff,
    depth5,
    trade,
};

enum class BookSide : std::uint8_t {
    bid,
    ask,
};

enum class SpotParseError : std::uint8_t {
    none,
    invalid_input_buffer,
    payload_too_large,
    json_nesting_too_deep,
    malformed_json,
    root_not_object,
    missing_field,
    duplicate_field,
    wrong_type,
    unexpected_event_type,
    symbol_mismatch,
    invalid_decimal,
    non_positive_price,
    invalid_quantity,
    invalid_update_range,
    too_many_levels,
    invalid_level_shape,
    duplicate_price,
    invalid_level_order,
    crossed_book,
    internal_capacity_error,
};

enum class SpotField : std::uint8_t {
    none,
    root,
    event_type,
    event_time,
    symbol,
    first_update_id,
    final_update_id,
    last_update_id,
    bids,
    asks,
    price,
    quantity,
    level,
};

struct PaddedJsonView {
    const char* data{nullptr};
    std::size_t size{0};
    std::size_t capacity{0};
};

struct LevelUpdate {
    std::int64_t price{0};
    std::int64_t quantity{0};
    BookSide side{BookSide::bid};
};

struct LevelUpdateRange {
    const LevelUpdate* data{nullptr};
    std::size_t size{0};

    [[nodiscard]] const LevelUpdate* begin() const noexcept {
        return data;
    }

    [[nodiscard]] const LevelUpdate* end() const noexcept {
        return data == nullptr ? nullptr : data + size;
    }

    [[nodiscard]] const LevelUpdate& operator[](
        const std::size_t index) const noexcept {
        return data[index];
    }
};

struct SpotDepthEvent {
    bool has_event_time{false};
    std::uint64_t event_time_ms{0};
    std::uint64_t first_update_id{0};
    std::uint64_t final_update_id{0};
    LevelUpdateRange bids{};
    LevelUpdateRange asks{};
};

struct SpotTradeAuditEvent {
    bool event_type_present{false};
    bool event_type_matches{true};
    bool symbol_present{false};
    bool symbol_matches{true};
};

struct SpotEvent {
    SpotStreamKind kind{SpotStreamKind::depth_diff};
    SpotDepthEvent depth{};
    SpotTradeAuditEvent trade{};
};

struct SpotParseResult {
    SpotEvent event{};
    SpotParseError error{SpotParseError::none};
    SpotField field{SpotField::none};
    DecimalParseError decimal_error{DecimalParseError::none};

    [[nodiscard]] bool has_value() const noexcept {
        return error == SpotParseError::none;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value();
    }
};

class SpotPayloadParser {
public:
    SpotPayloadParser();
    ~SpotPayloadParser();

    SpotPayloadParser(const SpotPayloadParser&) = delete;
    SpotPayloadParser& operator=(const SpotPayloadParser&) = delete;
    SpotPayloadParser(SpotPayloadParser&&) noexcept;
    SpotPayloadParser& operator=(SpotPayloadParser&&) noexcept;

    // Single-processing-thread component. All LevelUpdateRange values in a
    // result remain valid only until the next parse call on this instance.
    [[nodiscard]] SpotParseResult parse(
        SpotStreamKind kind,
        std::string_view expected_symbol,
        PaddedJsonView payload) noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const SpotParseError error) noexcept {
    switch (error) {
        case SpotParseError::none:
            return "none";
        case SpotParseError::invalid_input_buffer:
            return "invalid_input_buffer";
        case SpotParseError::payload_too_large:
            return "payload_too_large";
        case SpotParseError::json_nesting_too_deep:
            return "json_nesting_too_deep";
        case SpotParseError::malformed_json:
            return "malformed_json";
        case SpotParseError::root_not_object:
            return "root_not_object";
        case SpotParseError::missing_field:
            return "missing_field";
        case SpotParseError::duplicate_field:
            return "duplicate_field";
        case SpotParseError::wrong_type:
            return "wrong_type";
        case SpotParseError::unexpected_event_type:
            return "unexpected_event_type";
        case SpotParseError::symbol_mismatch:
            return "symbol_mismatch";
        case SpotParseError::invalid_decimal:
            return "invalid_decimal";
        case SpotParseError::non_positive_price:
            return "non_positive_price";
        case SpotParseError::invalid_quantity:
            return "invalid_quantity";
        case SpotParseError::invalid_update_range:
            return "invalid_update_range";
        case SpotParseError::too_many_levels:
            return "too_many_levels";
        case SpotParseError::invalid_level_shape:
            return "invalid_level_shape";
        case SpotParseError::duplicate_price:
            return "duplicate_price";
        case SpotParseError::invalid_level_order:
            return "invalid_level_order";
        case SpotParseError::crossed_book:
            return "crossed_book";
        case SpotParseError::internal_capacity_error:
            return "internal_capacity_error";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const SpotField field) noexcept {
    switch (field) {
        case SpotField::none:
            return "none";
        case SpotField::root:
            return "root";
        case SpotField::event_type:
            return "event_type";
        case SpotField::event_time:
            return "event_time";
        case SpotField::symbol:
            return "symbol";
        case SpotField::first_update_id:
            return "first_update_id";
        case SpotField::final_update_id:
            return "final_update_id";
        case SpotField::last_update_id:
            return "last_update_id";
        case SpotField::bids:
            return "bids";
        case SpotField::asks:
            return "asks";
        case SpotField::price:
            return "price";
        case SpotField::quantity:
            return "quantity";
        case SpotField::level:
            return "level";
    }
    return "unknown";
}

}  // namespace hft
