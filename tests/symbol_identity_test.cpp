#include "test_framework.h"

#include "hft/symbol_identity.h"

#include <array>
#include <cstddef>
#include <string>
#include <string_view>

namespace hft::test {
namespace {

void test_instrument_id_derivation(Context& context) {
    const InstrumentIdResult bitcoin =
        derive_instrument_id("BTCUSDT");
    context.expect(
        bitcoin.has_value() &&
            bitcoin.value == 1'747'767'916,
        "BTCUSDT instrument ID matches exact 32-bit FNV-1a contract");
    context.expect(
        derive_instrument_id("ETHUSDT").value == 1'617'942'128,
        "ETHUSDT instrument ID is stable");

    for (const std::string_view invalid :
         {"", "btcusdt", "BTC-USDT", "BTC,USDT"}) {
        const InstrumentIdResult result =
            derive_instrument_id(invalid);
        context.expect(
            !result &&
                result.error == SymbolValidationError::invalid_symbol &&
                result.value == 0,
            std::string{"invalid normalized symbol is rejected: "} +
                std::string{invalid});
    }

    const std::string too_long(33U, 'A');
    context.expect(
        !derive_instrument_id(too_long),
        "symbol longer than 32 bytes is rejected");
}

void test_symbol_set_validation(Context& context) {
    constexpr std::array<std::string_view, 2U> symbols{
        "BTCUSDT", "ETHUSDT"};
    constexpr std::array<std::string_view, 2U> reversed{
        "ETHUSDT", "BTCUSDT"};
    context.expect(
        validate_symbol_set(symbols.data(), symbols.size()).has_value(),
        "valid normalized symbol set is accepted");
    context.expect(
        validate_symbol_set(reversed.data(), reversed.size()).has_value(),
        "symbol-set validation is independent of input order");

    constexpr std::array<std::string_view, 2U> duplicate{
        "BTCUSDT", "BTCUSDT"};
    const SymbolSetValidationResult duplicate_result =
        validate_symbol_set(duplicate.data(), duplicate.size());
    context.expect(
        duplicate_result.error ==
                SymbolValidationError::duplicate_symbol &&
            duplicate_result.first_index == 0U &&
            duplicate_result.second_index == 1U,
        "duplicate normalized symbols report both indices");

    // These two normalized symbols collide after the documented sign-bit
    // clearing step even though their full 32-bit FNV-1a hashes differ.
    constexpr std::array<std::string_view, 2U> collision{
        "S21359", "S122546"};
    const SymbolSetValidationResult collision_result =
        validate_symbol_set(collision.data(), collision.size());
    context.expect(
        collision_result.error ==
                SymbolValidationError::instrument_id_collision &&
            collision_result.first_index == 0U &&
            collision_result.second_index == 1U,
        "instrument-ID collision is rejected deterministically");

    std::array<std::string_view, kMaxConfiguredSymbols + 1U> excessive{};
    excessive.fill("BTCUSDT");
    context.expect(
        validate_symbol_set(excessive.data(), excessive.size()).error ==
            SymbolValidationError::too_many_symbols,
        "more than 32 configured symbols is rejected before traversal");
    context.expect(
        validate_symbol_set(nullptr, 1U).error ==
            SymbolValidationError::invalid_input,
        "nonempty null symbol set is rejected");
    context.expect(
        validate_symbol_set(nullptr, 0U).error ==
            SymbolValidationError::empty_symbol_set,
        "empty configured symbol set is rejected");
}

}  // namespace

void run_symbol_identity_tests(Context& context) {
    test_instrument_id_derivation(context);
    test_symbol_set_validation(context);
}

}  // namespace hft::test
