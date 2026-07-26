#include "test_framework.h"

#include "hft/fixed_point.h"

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace hft::test {
namespace {

struct AcceptedCase {
    std::string_view input;
    std::int64_t expected;
};

struct RejectedCase {
    std::string_view input;
    DecimalParseError expected;
};

void expect_accepted(
    Context& context,
    const std::string_view input,
    const std::int64_t expected) {
    const DecimalParseResult result = parse_decimal_1e8(input);
    const std::string prefix = "parse accepted input '" + std::string{input} + "'";

    context.expect(result.has_value(), prefix + " reports success");
    context.expect(result.error == DecimalParseError::none,
                   prefix + " reports no error");
    context.expect(result.value == expected, prefix + " has the scaled value");
}

void expect_rejected(
    Context& context,
    const std::string_view input,
    const DecimalParseError expected) {
    const DecimalParseResult result = parse_decimal_1e8(input);
    const std::string prefix = "reject input '" + std::string{input} + "'";

    context.expect(!result.has_value(), prefix + " reports failure");
    context.expect(result.error == expected,
                   prefix + " expected error " +
                       std::string{to_string(expected)} + ", actual " +
                       std::string{to_string(result.error)});
    context.expect(result.value == 0, prefix + " exposes no partial value");
}

}  // namespace

void run_fixed_point_tests(Context& context) {
    constexpr std::array accepted{
        AcceptedCase{"0", 0},
        AcceptedCase{"000000", 0},
        AcceptedCase{"1", 100'000'000},
        AcceptedCase{"1.1", 110'000'000},
        AcceptedCase{"1.12", 112'000'000},
        AcceptedCase{"1.123", 112'300'000},
        AcceptedCase{"1.1234", 112'340'000},
        AcceptedCase{"1.12345", 112'345'000},
        AcceptedCase{"1.123456", 112'345'600},
        AcceptedCase{"1.1234567", 112'345'670},
        AcceptedCase{"1.12345678", 112'345'678},
        AcceptedCase{"0.00000001", 1},
        AcceptedCase{"1.250000000", 125'000'000},
        AcceptedCase{"1.2500000000000000", 125'000'000},
        AcceptedCase{"0001.25000000", 125'000'000},
        AcceptedCase{"0000000000000000000000000000000000000001",
                     100'000'000},
        AcceptedCase{"92233720368.54775806",
                     std::numeric_limits<std::int64_t>::max() - 1},
        AcceptedCase{"92233720368.54775807",
                     std::numeric_limits<std::int64_t>::max()},
        AcceptedCase{"00092233720368.54775807000",
                     std::numeric_limits<std::int64_t>::max()},
    };

    for (const AcceptedCase& test_case : accepted) {
        expect_accepted(context, test_case.input, test_case.expected);
    }

    constexpr std::array rejected{
        RejectedCase{"", DecimalParseError::empty},
        RejectedCase{".", DecimalParseError::missing_integer_digits},
        RejectedCase{".1", DecimalParseError::missing_integer_digits},
        RejectedCase{"1.", DecimalParseError::missing_fractional_digits},
        RejectedCase{"+1", DecimalParseError::invalid_character},
        RejectedCase{"-1", DecimalParseError::invalid_character},
        RejectedCase{" 1", DecimalParseError::invalid_character},
        RejectedCase{"1 ", DecimalParseError::invalid_character},
        RejectedCase{"1\t", DecimalParseError::invalid_character},
        RejectedCase{"\n1", DecimalParseError::invalid_character},
        RejectedCase{"1\r", DecimalParseError::invalid_character},
        RejectedCase{"1e3", DecimalParseError::invalid_character},
        RejectedCase{"1E3", DecimalParseError::invalid_character},
        RejectedCase{"1..0", DecimalParseError::invalid_character},
        RejectedCase{"1a", DecimalParseError::invalid_character},
        RejectedCase{std::string_view{"1\0", 2},
                     DecimalParseError::invalid_character},
        RejectedCase{std::string_view{"1\xC2\xA0", 3},
                     DecimalParseError::invalid_character},
        RejectedCase{"0.000000001",
                     DecimalParseError::non_zero_discarded_digit},
        RejectedCase{"1.123456789",
                     DecimalParseError::non_zero_discarded_digit},
        RejectedCase{"92233720368.547758071",
                     DecimalParseError::non_zero_discarded_digit},
        RejectedCase{"92233720368.54775808", DecimalParseError::overflow},
        RejectedCase{"92233720369", DecimalParseError::overflow},
        RejectedCase{"999999999999999999999999999999",
                     DecimalParseError::overflow},
    };

    for (const RejectedCase& test_case : rejected) {
        expect_rejected(context, test_case.input, test_case.expected);
    }
}

}  // namespace hft::test
