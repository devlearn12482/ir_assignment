#pragma once

#include <cstdint>
#include <string_view>

namespace hft {

inline constexpr std::int64_t kDecimalScale{100'000'000};

enum class DecimalParseError : std::uint8_t {
    none,
    empty,
    missing_integer_digits,
    missing_fractional_digits,
    invalid_character,
    non_zero_discarded_digit,
    overflow,
};

struct DecimalParseResult {
    std::int64_t value{0};
    DecimalParseError error{DecimalParseError::none};

    [[nodiscard]] constexpr bool has_value() const noexcept {
        return error == DecimalParseError::none;
    }

    [[nodiscard]] constexpr explicit operator bool() const noexcept {
        return has_value();
    }
};

[[nodiscard]] DecimalParseResult parse_decimal_1e8(
    std::string_view input) noexcept;

[[nodiscard]] constexpr std::string_view to_string(
    const DecimalParseError error) noexcept {
    switch (error) {
        case DecimalParseError::none:
            return "none";
        case DecimalParseError::empty:
            return "empty";
        case DecimalParseError::missing_integer_digits:
            return "missing_integer_digits";
        case DecimalParseError::missing_fractional_digits:
            return "missing_fractional_digits";
        case DecimalParseError::invalid_character:
            return "invalid_character";
        case DecimalParseError::non_zero_discarded_digit:
            return "non_zero_discarded_digit";
        case DecimalParseError::overflow:
            return "overflow";
    }
    return "unknown";
}

}  // namespace hft
