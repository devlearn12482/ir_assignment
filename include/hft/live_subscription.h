#pragma once

#include "hft/spot_payload_parser.h"
#include "hft/symbol_identity.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace hft {

inline constexpr std::size_t kStreamsPerSymbol{3U};
inline constexpr std::size_t kMaxDerivedStreams{
    kMaxConfiguredSymbols * kStreamsPerSymbol};
inline constexpr std::size_t kMaxCombinedTargetBytes{8192U};
inline constexpr std::size_t kMaxEnvelopeStreamNameBytes{128U};

struct VenueEndpoint {
    std::string_view host{};
    std::string_view port{};
    std::string_view combined_target_prefix{};
};

enum class SubscriptionErrorCode : std::uint8_t {
    none,
    invalid_venue,
    invalid_symbols,
    stream_count_exceeded,
    target_size_overflow,
    target_too_large,
    allocation_failure,
    route_table_failure,
};

struct SubscriptionError {
    SubscriptionErrorCode code{SubscriptionErrorCode::none};
    SymbolValidationError symbol_error{SymbolValidationError::none};
    std::size_t first_index{kNoSymbolIndex};
    std::size_t second_index{kNoSymbolIndex};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != SubscriptionErrorCode::none;
    }
};

struct CombinedTargetSizeResult {
    std::size_t bytes{0};
    SubscriptionErrorCode error{SubscriptionErrorCode::none};

    [[nodiscard]] bool success() const noexcept {
        return error == SubscriptionErrorCode::none;
    }
};

struct LiveRoute {
    std::string stream_name{};
    std::string_view normalized_symbol{};
    std::size_t symbol_index{kNoSymbolIndex};
    SpotStreamKind stream_kind{SpotStreamKind::depth_diff};
};

enum class RouteLookupError : std::uint8_t {
    none,
    unknown_stream,
    stream_name_too_long,
};

struct RouteLookupResult {
    const LiveRoute* route{nullptr};
    RouteLookupError error{RouteLookupError::unknown_stream};

    [[nodiscard]] bool success() const noexcept {
        return route != nullptr && error == RouteLookupError::none;
    }
};

[[nodiscard]] VenueEndpoint production_endpoint(
    PayloadVenue venue) noexcept;

[[nodiscard]] CombinedTargetSizeResult checked_combined_target_size(
    std::size_t target_prefix_bytes,
    const std::string_view* normalized_symbols,
    std::size_t symbol_count) noexcept;

class LiveSubscription {
public:
    [[nodiscard]] static std::unique_ptr<LiveSubscription> create(
        PayloadVenue venue,
        const std::string_view* normalized_symbols,
        std::size_t symbol_count,
        SubscriptionError& error) noexcept;

    LiveSubscription(const LiveSubscription&) = delete;
    LiveSubscription& operator=(const LiveSubscription&) = delete;
    LiveSubscription(LiveSubscription&&) = delete;
    LiveSubscription& operator=(LiveSubscription&&) = delete;

    [[nodiscard]] PayloadVenue venue() const noexcept;
    [[nodiscard]] const VenueEndpoint& endpoint() const noexcept;
    [[nodiscard]] std::string_view target() const noexcept;
    [[nodiscard]] std::size_t symbol_count() const noexcept;
    [[nodiscard]] std::string_view symbol(
        std::size_t index) const noexcept;
    [[nodiscard]] std::size_t route_count() const noexcept;
    [[nodiscard]] const LiveRoute* route_at(
        std::size_t index) const noexcept;
    [[nodiscard]] RouteLookupResult find_route(
        std::string_view stream_name) const noexcept;

private:
    static constexpr std::size_t kLookupSlotCount{256U};
    static constexpr std::size_t kLookupSlotMask{
        kLookupSlotCount - 1U};

    explicit LiveSubscription(PayloadVenue venue) noexcept;

    [[nodiscard]] bool insert_route(
        std::size_t route_index) noexcept;

    PayloadVenue venue_{PayloadVenue::spot};
    VenueEndpoint endpoint_{};
    std::string target_{};
    std::array<std::string, kMaxConfiguredSymbols> symbols_{};
    std::size_t symbol_count_{0};
    std::array<LiveRoute, kMaxDerivedStreams> routes_{};
    std::size_t route_count_{0};
    std::array<std::size_t, kLookupSlotCount> lookup_{};
};

[[nodiscard]] constexpr std::string_view to_string(
    const SubscriptionErrorCode code) noexcept {
    switch (code) {
        case SubscriptionErrorCode::none:
            return "none";
        case SubscriptionErrorCode::invalid_venue:
            return "invalid_venue";
        case SubscriptionErrorCode::invalid_symbols:
            return "invalid_symbols";
        case SubscriptionErrorCode::stream_count_exceeded:
            return "stream_count_exceeded";
        case SubscriptionErrorCode::target_size_overflow:
            return "target_size_overflow";
        case SubscriptionErrorCode::target_too_large:
            return "target_too_large";
        case SubscriptionErrorCode::allocation_failure:
            return "allocation_failure";
        case SubscriptionErrorCode::route_table_failure:
            return "route_table_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const RouteLookupError error) noexcept {
    switch (error) {
        case RouteLookupError::none:
            return "none";
        case RouteLookupError::unknown_stream:
            return "unknown_stream";
        case RouteLookupError::stream_name_too_long:
            return "stream_name_too_long";
    }
    return "unknown";
}

}  // namespace hft
