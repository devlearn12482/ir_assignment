#include "hft/fixed_point.h"

#include <cstddef>
#include <cstdint>
#include <limits>

namespace hft {
namespace {

[[nodiscard]] constexpr bool is_ascii_digit(const char value) noexcept {
    return value >= '0' && value <= '9';
}

[[nodiscard]] constexpr std::int64_t digit_value(const char value) noexcept {
    return static_cast<std::int64_t>(value - '0');
}

[[nodiscard]] constexpr DecimalParseResult failure(
    const DecimalParseError error) noexcept {
    return DecimalParseResult{0, error};
}

}  // namespace

DecimalParseResult parse_decimal_1e8(const std::string_view input) noexcept {
    if (input.empty()) {
        return failure(DecimalParseError::empty);
    }
    if (input.front() == '.') {
        return failure(DecimalParseError::missing_integer_digits);
    }

    constexpr std::int64_t maximum =
        std::numeric_limits<std::int64_t>::max();
    constexpr std::int64_t maximum_whole = maximum / kDecimalScale;

    std::int64_t whole{0};
    std::size_t position{0};

    while (position < input.size() && input[position] != '.') {
        const char character = input[position];
        if (!is_ascii_digit(character)) {
            return failure(DecimalParseError::invalid_character);
        }

        const std::int64_t digit = digit_value(character);
        if (whole > (maximum_whole - digit) / 10) {
            return failure(DecimalParseError::overflow);
        }
        whole = (whole * 10) + digit;
        ++position;
    }

    std::int64_t fraction{0};
    std::size_t fractional_digits{0};

    if (position < input.size()) {
        ++position;
        if (position == input.size()) {
            return failure(DecimalParseError::missing_fractional_digits);
        }

        for (; position < input.size(); ++position) {
            const char character = input[position];
            if (!is_ascii_digit(character)) {
                return failure(DecimalParseError::invalid_character);
            }

            if (fractional_digits < 8) {
                fraction = (fraction * 10) + digit_value(character);
            } else if (character != '0') {
                return failure(DecimalParseError::non_zero_discarded_digit);
            }
            ++fractional_digits;
        }

        while (fractional_digits < 8) {
            fraction *= 10;
            ++fractional_digits;
        }
    }

    if (whole > (maximum - fraction) / kDecimalScale) {
        return failure(DecimalParseError::overflow);
    }

    return DecimalParseResult{
        (whole * kDecimalScale) + fraction,
        DecimalParseError::none};
}

}  // namespace hft
