#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hft {

inline constexpr std::size_t kMaxNormalizedSymbolBytes{32U};
inline constexpr std::size_t kMaxConfiguredSymbols{32U};
inline constexpr std::size_t kNoSymbolIndex{
    static_cast<std::size_t>(-1)};

enum class SymbolValidationError : std::uint8_t {
    none,
    invalid_input,
    empty_symbol_set,
    too_many_symbols,
    invalid_symbol,
    duplicate_symbol,
    instrument_id_collision,
};

struct InstrumentIdResult {
    std::int32_t value{0};
    SymbolValidationError error{SymbolValidationError::none};

    [[nodiscard]] bool has_value() const noexcept {
        return error == SymbolValidationError::none;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value();
    }
};

struct SymbolSetValidationResult {
    SymbolValidationError error{SymbolValidationError::none};
    std::size_t first_index{kNoSymbolIndex};
    std::size_t second_index{kNoSymbolIndex};

    [[nodiscard]] bool has_value() const noexcept {
        return error == SymbolValidationError::none;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value();
    }
};

[[nodiscard]] bool is_normalized_symbol(
    std::string_view symbol) noexcept;
[[nodiscard]] InstrumentIdResult derive_instrument_id(
    std::string_view normalized_symbol) noexcept;
[[nodiscard]] SymbolSetValidationResult validate_symbol_set(
    const std::string_view* normalized_symbols,
    std::size_t count) noexcept;

[[nodiscard]] constexpr std::string_view to_string(
    const SymbolValidationError error) noexcept {
    switch (error) {
        case SymbolValidationError::none:
            return "none";
        case SymbolValidationError::invalid_input:
            return "invalid_input";
        case SymbolValidationError::empty_symbol_set:
            return "empty_symbol_set";
        case SymbolValidationError::too_many_symbols:
            return "too_many_symbols";
        case SymbolValidationError::invalid_symbol:
            return "invalid_symbol";
        case SymbolValidationError::duplicate_symbol:
            return "duplicate_symbol";
        case SymbolValidationError::instrument_id_collision:
            return "instrument_id_collision";
    }
    return "unknown";
}

}  // namespace hft
