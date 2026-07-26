#include "test_framework.h"

#include "hft/live_subscription.h"

#include <array>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

namespace hft::test {
namespace {

void test_endpoint_and_single_symbol(Context& context) {
    const VenueEndpoint spot =
        production_endpoint(PayloadVenue::spot);
    context.expect(
        spot.host == "stream.binance.com" &&
            spot.port == "9443" &&
            spot.combined_target_prefix == "/stream?streams=",
        "Spot production endpoint is exact");

    const VenueEndpoint usdm =
        production_endpoint(PayloadVenue::usdm);
    context.expect(
        usdm.host == "fstream.binance.com" &&
            usdm.port == "443" &&
            usdm.combined_target_prefix ==
                "/public/stream?streams=",
        "USD-M assignment endpoint is exact");

    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    SubscriptionError error;
    std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            error);
    context.expect(
        subscription != nullptr && !error,
        "one-symbol subscription is created");
    if (!subscription) {
        return;
    }
    context.expect(
        subscription->target() ==
            "/stream?streams=btcusdt@depth@100ms/"
            "btcusdt@depth5@100ms/btcusdt@trade",
        "combined target contains all three exact streams");
    context.expect(
        subscription->symbol_count() == 1U &&
            subscription->route_count() == 3U &&
            subscription->symbol(0U) == "BTCUSDT" &&
            subscription->symbol(1U).empty(),
        "subscription exposes stable symbol and route bounds");

    const RouteLookupResult depth =
        subscription->find_route("btcusdt@depth@100ms");
    const RouteLookupResult depth5 =
        subscription->find_route("btcusdt@depth5@100ms");
    const RouteLookupResult trade =
        subscription->find_route("btcusdt@trade");
    context.expect(
        depth.success() &&
            depth.route->stream_kind ==
                SpotStreamKind::depth_diff &&
            depth.route->symbol_index == 0U &&
            depth.route->normalized_symbol == "BTCUSDT",
        "differential route resolves without mutation");
    context.expect(
        depth5.success() &&
            depth5.route->stream_kind == SpotStreamKind::depth5,
        "partial-depth route resolves");
    context.expect(
        trade.success() &&
            trade.route->stream_kind == SpotStreamKind::trade,
        "trade route resolves");
}

void test_multi_symbol_order_and_lookup(Context& context) {
    constexpr std::array<std::string_view, 2U> symbols{
        "ETHUSDT", "BTCUSDT"};
    SubscriptionError error;
    const std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::usdm,
            symbols.data(),
            symbols.size(),
            error);
    context.expect(
        subscription != nullptr && !error,
        "multi-symbol USD-M subscription is created");
    if (!subscription) {
        return;
    }
    context.expect(
        subscription->target().substr(0U, 23U) ==
            "/public/stream?streams=" &&
            subscription->route_count() == 6U,
        "USD-M target uses one connection and six routes");
    const LiveRoute* fourth = subscription->route_at(3U);
    context.expect(
        fourth != nullptr &&
            fourth->stream_name == "btcusdt@depth@100ms" &&
            fourth->symbol_index == 1U,
        "configured symbol ordering gives stable target indices");
    context.expect(
        subscription->route_at(6U) == nullptr,
        "route accessor is bounded");
}

void test_configuration_rejections(Context& context) {
    constexpr std::array<std::string_view, 2U> duplicate{
        "BTCUSDT", "BTCUSDT"};
    SubscriptionError error;
    context.expect(
        !LiveSubscription::create(
            PayloadVenue::spot,
            duplicate.data(),
            duplicate.size(),
            error) &&
            error.code == SubscriptionErrorCode::invalid_symbols &&
            error.symbol_error ==
                SymbolValidationError::duplicate_symbol,
        "duplicate symbols are rejected before construction");

    constexpr std::array<std::string_view, 2U> collision{
        "S21359", "S122546"};
    context.expect(
        !LiveSubscription::create(
            PayloadVenue::spot,
            collision.data(),
            collision.size(),
            error) &&
            error.symbol_error ==
                SymbolValidationError::instrument_id_collision,
        "instrument-ID collision is rejected before routes exist");

    constexpr std::array<std::string_view, 1U> lowercase{
        "btcusdt"};
    context.expect(
        !LiveSubscription::create(
            PayloadVenue::spot,
            lowercase.data(),
            lowercase.size(),
            error) &&
            error.symbol_error ==
                SymbolValidationError::invalid_symbol,
        "subscription requires normalized symbols");

    std::array<std::string, kMaxConfiguredSymbols + 1U> owned{};
    std::array<std::string_view, kMaxConfiguredSymbols + 1U> excessive{};
    for (std::size_t index = 0; index < excessive.size(); ++index) {
        owned[index] = "S" + std::to_string(index);
        excessive[index] = owned[index];
    }
    const std::unique_ptr<LiveSubscription> maximum =
        LiveSubscription::create(
            PayloadVenue::spot,
            excessive.data(),
            kMaxConfiguredSymbols,
            error);
    context.expect(
        maximum != nullptr &&
            maximum->symbol_count() == kMaxConfiguredSymbols &&
            maximum->route_count() == kMaxDerivedStreams,
        "exactly 32 symbols derive exactly 96 routes");
    context.expect(
        !LiveSubscription::create(
            PayloadVenue::spot,
            excessive.data(),
            excessive.size(),
            error) &&
            error.symbol_error ==
                SymbolValidationError::too_many_symbols,
        "the 33rd symbol is rejected all-or-nothing");

    constexpr std::array<std::string_view, 1U> valid{"BTCUSDT"};
    context.expect(
        !LiveSubscription::create(
            static_cast<PayloadVenue>(255U),
            valid.data(),
            valid.size(),
            error) &&
            error.code == SubscriptionErrorCode::invalid_venue,
        "invalid venue is rejected before symbol traversal");
}

void test_size_and_stream_name_boundaries(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{"A"};
    const CombinedTargetSizeResult base =
        checked_combined_target_size(
            0U, symbols.data(), symbols.size());
    context.expect(
        base.success() && base.bytes == 36U,
        "pure target sizing includes suffixes and separators");

    const std::size_t exact_prefix =
        kMaxCombinedTargetBytes - base.bytes;
    const CombinedTargetSizeResult exact =
        checked_combined_target_size(
            exact_prefix, symbols.data(), symbols.size());
    const CombinedTargetSizeResult one_over =
        checked_combined_target_size(
            exact_prefix + 1U, symbols.data(), symbols.size());
    context.expect(
        exact.success() &&
            exact.bytes == kMaxCombinedTargetBytes &&
            one_over.success() &&
            one_over.bytes == kMaxCombinedTargetBytes + 1U,
        "target helper reports exact and one-over policy boundaries");

    const CombinedTargetSizeResult overflow =
        checked_combined_target_size(
            std::numeric_limits<std::size_t>::max(),
            symbols.data(),
            symbols.size());
    context.expect(
        overflow.error ==
            SubscriptionErrorCode::target_size_overflow,
        "target sizing detects size_t overflow");

    SubscriptionError error;
    const std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            error);
    if (!subscription) {
        context.expect(false, "boundary lookup subscription exists");
        return;
    }
    const std::string exact_name(
        kMaxEnvelopeStreamNameBytes, 'a');
    const std::string long_name(
        kMaxEnvelopeStreamNameBytes + 1U, 'a');
    context.expect(
        subscription->find_route(exact_name).error ==
            RouteLookupError::unknown_stream,
        "128-byte stream reaches normal lookup");
    context.expect(
        subscription->find_route(long_name).error ==
            RouteLookupError::stream_name_too_long,
        "129-byte stream is rejected before hashing traversal");
}

}  // namespace

void run_live_subscription_tests(Context& context) {
    test_endpoint_and_single_symbol(context);
    test_multi_symbol_order_and_lookup(context);
    test_configuration_rejections(context);
    test_size_and_stream_name_boundaries(context);
}

}  // namespace hft::test
