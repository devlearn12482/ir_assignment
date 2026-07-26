#include "test_framework.h"

#include "hft/live_subscription.h"
#include "hft/verified_websocket_session.h"

#include <boost/asio/io_context.hpp>

#include <array>
#include <memory>
#include <stdexcept>
#include <string_view>

namespace hft::test {
namespace {

void test_production_endpoint_mapping(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    SubscriptionError subscription_error;
    const std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            subscription_error);
    context.expect(
        subscription != nullptr,
        "verified session production subscription is created");
    if (!subscription) {
        return;
    }

    const VerifiedWebSocketEndpoint endpoint =
        production_websocket_endpoint(*subscription);
    context.expect(
        endpoint.connect_host == "stream.binance.com" &&
            endpoint.expected_hostname == "stream.binance.com" &&
            endpoint.port == "9443" &&
            endpoint.target == subscription->target() &&
            endpoint.trust_store == TrustStoreKind::system &&
            endpoint.test_ca_file.empty(),
        "production endpoint binds resolution, SNI, verification, and Host "
        "identity to the centralized venue host");
}

void test_configuration_fails_before_network(Context& context) {
    boost::asio::io_context io_context;
    WebSocketSessionResult error;
    const std::shared_ptr<VerifiedWebSocketSession> invalid =
        VerifiedWebSocketSession::create(
            io_context,
            VerifiedWebSocketEndpoint{},
            {},
            error);
    context.expect(
        !invalid &&
            error.code ==
                WebSocketSessionErrorCode::invalid_configuration &&
            error.stage == WebSocketSessionStage::configuration,
        "empty endpoint fails before asynchronous work");

    const std::shared_ptr<VerifiedWebSocketSession> missing_ca =
        VerifiedWebSocketSession::create(
            io_context,
            test_websocket_endpoint(
                "127.0.0.1",
                "443",
                "localhost",
                "/stream?streams=btcusdt@trade",
                "/definitely/not/a/real/test-ca.pem"),
            {},
            error);
    context.expect(
        !missing_ca &&
            error.code ==
                WebSocketSessionErrorCode::trust_store_failure &&
            error.stage == WebSocketSessionStage::configuration &&
            static_cast<bool>(error.native_error),
        "missing test CA fails before DNS or socket construction");
}

void test_cancel_reports_active_stage(Context& context) {
    boost::asio::io_context io_context;
    WebSocketSessionResult create_error;
    WebSocketSessionResult terminal_result;
    bool terminal_called = false;
    WebSocketSessionCallbacks callbacks;
    callbacks.on_terminal =
        [&](const WebSocketSessionResult result) {
            terminal_called = true;
            terminal_result = result;
            throw std::runtime_error{
                "terminal callback test exception"};
        };
    const std::shared_ptr<VerifiedWebSocketSession> session =
        VerifiedWebSocketSession::create(
            io_context,
            VerifiedWebSocketEndpoint{
                "localhost",
                "9",
                "localhost",
                "/stream?streams=btcusdt@trade",
                TrustStoreKind::system,
                {}},
            std::move(callbacks),
            create_error);
    context.expect(
        session != nullptr,
        "valid cancellation test session is created");
    if (!session) {
        return;
    }

    session->start();
    session->stop();
    io_context.run();
    context.expect(
        terminal_called &&
            terminal_result.code ==
                WebSocketSessionErrorCode::cancelled &&
            terminal_result.stage ==
                WebSocketSessionStage::resolve &&
            session->terminal(),
        "cancellation reports the in-flight resolve stage and contains "
        "terminal callback exceptions");
}

}  // namespace

void run_verified_websocket_session_tests(Context& context) {
    test_production_endpoint_mapping(context);
    test_configuration_fails_before_network(context);
    test_cancel_reports_active_stage(context);
}

}  // namespace hft::test
