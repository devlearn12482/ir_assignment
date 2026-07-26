#include "hft/live_subscription.h"

#include <array>
#include <limits>
#include <new>
#include <utility>

namespace hft {
namespace {

constexpr std::string_view kSpotHost{"stream.binance.com"};
constexpr std::string_view kSpotPort{"9443"};
constexpr std::string_view kSpotTargetPrefix{"/stream?streams="};
constexpr std::string_view kUsdMHost{"fstream.binance.com"};
constexpr std::string_view kUsdMPort{"443"};
constexpr std::string_view kUsdMTargetPrefix{"/public/stream?streams="};

constexpr std::array<std::string_view, kStreamsPerSymbol> kSuffixes{
    "@depth@100ms", "@depth5@100ms", "@trade"};
constexpr std::array<SpotStreamKind, kStreamsPerSymbol> kKinds{
    SpotStreamKind::depth_diff,
    SpotStreamKind::depth5,
    SpotStreamKind::trade};

[[nodiscard]] bool checked_add(
    std::size_t& total,
    const std::size_t increment) noexcept {
    if (increment > std::numeric_limits<std::size_t>::max() - total) {
        return false;
    }
    total += increment;
    return true;
}

[[nodiscard]] std::uint64_t hash_stream(
    const std::string_view stream) noexcept {
    std::uint64_t hash{14'695'981'039'346'656'037ULL};
    for (const char byte : stream) {
        hash ^= static_cast<unsigned char>(byte);
        hash *= 1'099'511'628'211ULL;
    }
    return hash;
}

[[nodiscard]] std::string lowercase_symbol(
    const std::string_view normalized_symbol) {
    std::string result;
    result.reserve(normalized_symbol.size());
    for (const char byte : normalized_symbol) {
        result.push_back(
            byte >= 'A' && byte <= 'Z'
                ? static_cast<char>(byte - 'A' + 'a')
                : byte);
    }
    return result;
}

}  // namespace

VenueEndpoint production_endpoint(const PayloadVenue venue) noexcept {
    switch (venue) {
        case PayloadVenue::spot:
            return VenueEndpoint{
                kSpotHost, kSpotPort, kSpotTargetPrefix};
        case PayloadVenue::usdm:
            return VenueEndpoint{
                kUsdMHost, kUsdMPort, kUsdMTargetPrefix};
    }
    return {};
}

CombinedTargetSizeResult checked_combined_target_size(
    const std::size_t target_prefix_bytes,
    const std::string_view* const normalized_symbols,
    const std::size_t symbol_count) noexcept {
    if (normalized_symbols == nullptr || symbol_count == 0U ||
        symbol_count > kMaxConfiguredSymbols) {
        return CombinedTargetSizeResult{
            0U, SubscriptionErrorCode::invalid_symbols};
    }
    if (symbol_count >
        std::numeric_limits<std::size_t>::max() /
            kStreamsPerSymbol) {
        return CombinedTargetSizeResult{
            0U, SubscriptionErrorCode::stream_count_exceeded};
    }
    const std::size_t stream_count =
        symbol_count * kStreamsPerSymbol;
    if (stream_count > kMaxDerivedStreams) {
        return CombinedTargetSizeResult{
            0U, SubscriptionErrorCode::stream_count_exceeded};
    }

    std::size_t total = target_prefix_bytes;
    for (std::size_t symbol_index = 0;
         symbol_index < symbol_count;
         ++symbol_index) {
        for (const std::string_view suffix : kSuffixes) {
            if (!checked_add(
                    total, normalized_symbols[symbol_index].size()) ||
                !checked_add(total, suffix.size())) {
                return CombinedTargetSizeResult{
                    0U,
                    SubscriptionErrorCode::target_size_overflow};
            }
        }
    }
    if (!checked_add(total, stream_count - 1U)) {
        return CombinedTargetSizeResult{
            0U, SubscriptionErrorCode::target_size_overflow};
    }
    return CombinedTargetSizeResult{total, SubscriptionErrorCode::none};
}

LiveSubscription::LiveSubscription(
    const PayloadVenue venue) noexcept
    : venue_{venue}, endpoint_{production_endpoint(venue)} {
    lookup_.fill(kNoSymbolIndex);
}

std::unique_ptr<LiveSubscription> LiveSubscription::create(
    const PayloadVenue venue,
    const std::string_view* const normalized_symbols,
    const std::size_t symbol_count,
    SubscriptionError& error) noexcept {
    error = {};
    const VenueEndpoint endpoint = production_endpoint(venue);
    if (endpoint.host.empty()) {
        error.code = SubscriptionErrorCode::invalid_venue;
        return nullptr;
    }

    const SymbolSetValidationResult symbols =
        validate_symbol_set(normalized_symbols, symbol_count);
    if (!symbols) {
        error.code = SubscriptionErrorCode::invalid_symbols;
        error.symbol_error = symbols.error;
        error.first_index = symbols.first_index;
        error.second_index = symbols.second_index;
        return nullptr;
    }

    const CombinedTargetSizeResult target_size =
        checked_combined_target_size(
            endpoint.combined_target_prefix.size(),
            normalized_symbols,
            symbol_count);
    if (!target_size.success()) {
        error.code = target_size.error;
        return nullptr;
    }
    if (target_size.bytes > kMaxCombinedTargetBytes) {
        error.code = SubscriptionErrorCode::target_too_large;
        return nullptr;
    }

    try {
        std::unique_ptr<LiveSubscription> subscription{
            new LiveSubscription{venue}};
        subscription->symbol_count_ = symbol_count;
        subscription->route_count_ =
            symbol_count * kStreamsPerSymbol;
        subscription->target_.reserve(target_size.bytes);
        subscription->target_.append(
            endpoint.combined_target_prefix);

        std::size_t route_index = 0;
        for (std::size_t symbol_index = 0;
             symbol_index < symbol_count;
             ++symbol_index) {
            subscription->symbols_[symbol_index] =
                normalized_symbols[symbol_index];
            const std::string lowercase =
                lowercase_symbol(normalized_symbols[symbol_index]);
            for (std::size_t kind_index = 0;
                 kind_index < kStreamsPerSymbol;
                 ++kind_index) {
                if (route_index != 0U) {
                    subscription->target_.push_back('/');
                }
                LiveRoute& route =
                    subscription->routes_[route_index];
                route.stream_name.reserve(
                    lowercase.size() + kSuffixes[kind_index].size());
                route.stream_name.append(lowercase);
                route.stream_name.append(kSuffixes[kind_index]);
                route.normalized_symbol =
                    subscription->symbols_[symbol_index];
                route.symbol_index = symbol_index;
                route.stream_kind = kKinds[kind_index];
                subscription->target_.append(route.stream_name);
                if (!subscription->insert_route(route_index)) {
                    error.code =
                        SubscriptionErrorCode::route_table_failure;
                    return nullptr;
                }
                ++route_index;
            }
        }
        if (subscription->target_.size() != target_size.bytes) {
            error.code = SubscriptionErrorCode::route_table_failure;
            return nullptr;
        }
        return subscription;
    } catch (const std::bad_alloc&) {
        error.code = SubscriptionErrorCode::allocation_failure;
        return nullptr;
    }
}

PayloadVenue LiveSubscription::venue() const noexcept {
    return venue_;
}

const VenueEndpoint& LiveSubscription::endpoint() const noexcept {
    return endpoint_;
}

std::string_view LiveSubscription::target() const noexcept {
    return target_;
}

std::size_t LiveSubscription::symbol_count() const noexcept {
    return symbol_count_;
}

std::string_view LiveSubscription::symbol(
    const std::size_t index) const noexcept {
    return index < symbol_count_ ? std::string_view{symbols_[index]}
                                 : std::string_view{};
}

std::size_t LiveSubscription::route_count() const noexcept {
    return route_count_;
}

const LiveRoute* LiveSubscription::route_at(
    const std::size_t index) const noexcept {
    return index < route_count_ ? &routes_[index] : nullptr;
}

RouteLookupResult LiveSubscription::find_route(
    const std::string_view stream_name) const noexcept {
    if (stream_name.size() > kMaxEnvelopeStreamNameBytes) {
        return RouteLookupResult{
            nullptr, RouteLookupError::stream_name_too_long};
    }
    std::size_t slot =
        static_cast<std::size_t>(hash_stream(stream_name)) &
        kLookupSlotMask;
    for (std::size_t probe = 0; probe < kLookupSlotCount; ++probe) {
        const std::size_t route_index = lookup_[slot];
        if (route_index == kNoSymbolIndex) {
            return RouteLookupResult{
                nullptr, RouteLookupError::unknown_stream};
        }
        if (routes_[route_index].stream_name == stream_name) {
            return RouteLookupResult{
                &routes_[route_index], RouteLookupError::none};
        }
        slot = (slot + 1U) & kLookupSlotMask;
    }
    return RouteLookupResult{
        nullptr, RouteLookupError::unknown_stream};
}

bool LiveSubscription::insert_route(
    const std::size_t route_index) noexcept {
    std::size_t slot = static_cast<std::size_t>(
                           hash_stream(routes_[route_index].stream_name)) &
                       kLookupSlotMask;
    for (std::size_t probe = 0; probe < kLookupSlotCount; ++probe) {
        if (lookup_[slot] == kNoSymbolIndex) {
            lookup_[slot] = route_index;
            return true;
        }
        if (routes_[lookup_[slot]].stream_name ==
            routes_[route_index].stream_name) {
            return false;
        }
        slot = (slot + 1U) & kLookupSlotMask;
    }
    return false;
}

}  // namespace hft
