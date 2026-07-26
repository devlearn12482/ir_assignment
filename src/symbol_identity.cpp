#include "hft/symbol_identity.h"

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hft {
namespace {

inline constexpr std::uint32_t kFnvOffsetBasis{2'166'136'261U};
inline constexpr std::uint32_t kFnvPrime{16'777'619U};
inline constexpr std::uint32_t kPositiveIdMask{0x7fff'ffffU};

}  // namespace

bool is_normalized_symbol(
    const std::string_view symbol) noexcept {
    if (symbol.empty() ||
        symbol.size() > kMaxNormalizedSymbolBytes) {
        return false;
    }
    for (const char byte : symbol) {
        const bool uppercase = byte >= 'A' && byte <= 'Z';
        const bool digit = byte >= '0' && byte <= '9';
        if (!uppercase && !digit) {
            return false;
        }
    }
    return true;
}

InstrumentIdResult derive_instrument_id(
    const std::string_view normalized_symbol) noexcept {
    if (!is_normalized_symbol(normalized_symbol)) {
        return InstrumentIdResult{
            0, SymbolValidationError::invalid_symbol};
    }

    std::uint32_t hash = kFnvOffsetBasis;
    for (const char byte : normalized_symbol) {
        hash ^= static_cast<std::uint8_t>(byte);
        hash *= kFnvPrime;
    }
    std::uint32_t value = hash & kPositiveIdMask;
    if (value == 0U) {
        value = 1U;
    }
    return InstrumentIdResult{
        static_cast<std::int32_t>(value),
        SymbolValidationError::none};
}

SymbolSetValidationResult validate_symbol_set(
    const std::string_view* const normalized_symbols,
    const std::size_t count) noexcept {
    if (count == 0U) {
        return SymbolSetValidationResult{
            SymbolValidationError::empty_symbol_set};
    }
    if (count > kMaxConfiguredSymbols) {
        return SymbolSetValidationResult{
            SymbolValidationError::too_many_symbols};
    }
    if (normalized_symbols == nullptr) {
        return SymbolSetValidationResult{
            SymbolValidationError::invalid_input};
    }

    for (std::size_t index = 0; index < count; ++index) {
        const InstrumentIdResult current =
            derive_instrument_id(normalized_symbols[index]);
        if (!current) {
            return SymbolSetValidationResult{
                current.error, index, index};
        }

        for (std::size_t previous = 0; previous < index; ++previous) {
            if (normalized_symbols[previous] ==
                normalized_symbols[index]) {
                return SymbolSetValidationResult{
                    SymbolValidationError::duplicate_symbol,
                    previous,
                    index};
            }
            const InstrumentIdResult prior =
                derive_instrument_id(normalized_symbols[previous]);
            if (prior.value == current.value) {
                return SymbolSetValidationResult{
                    SymbolValidationError::instrument_id_collision,
                    previous,
                    index};
            }
        }
    }
    return {};
}

}  // namespace hft
