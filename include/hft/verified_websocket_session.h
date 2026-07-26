#pragma once

#include "hft/live_subscription.h"

#include <boost/asio/io_context.hpp>
#include <boost/system/error_code.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace hft {

enum class TrustStoreKind : std::uint8_t {
    system,
    test_ca_file,
};

struct VerifiedWebSocketEndpoint {
    std::string connect_host{};
    std::string port{};
    std::string expected_hostname{};
    std::string target{};
    TrustStoreKind trust_store{TrustStoreKind::system};
    std::string test_ca_file{};
};

enum class WebSocketSessionStage : std::uint8_t {
    none,
    configuration,
    resolve,
    connect,
    sni,
    tls_handshake,
    websocket_handshake,
    open,
    websocket_close,
};

enum class WebSocketSessionErrorCode : std::uint8_t {
    none,
    invalid_configuration,
    allocation_failure,
    trust_store_failure,
    tls_policy_failure,
    resolve_failure,
    connect_failure,
    sni_failure,
    tls_handshake_failure,
    websocket_handshake_failure,
    close_failure,
    callback_failure,
    timeout,
    cancelled,
};

struct WebSocketSessionResult {
    WebSocketSessionErrorCode code{
        WebSocketSessionErrorCode::none};
    WebSocketSessionStage stage{WebSocketSessionStage::none};
    boost::system::error_code native_error{};

    [[nodiscard]] bool success() const noexcept {
        return code == WebSocketSessionErrorCode::none;
    }
};

struct WebSocketSessionCallbacks {
    // Callbacks run on the session strand. Exceptions from on_open terminate
    // the session as callback_failure; exceptions from the terminal
    // notification are contained because the session is already terminal.
    std::function<void()> on_open{};
    std::function<void(WebSocketSessionResult)> on_terminal{};
};

[[nodiscard]] VerifiedWebSocketEndpoint production_websocket_endpoint(
    const LiveSubscription& subscription);

// Test-only endpoint construction retains certificate-chain and hostname
// verification. There is deliberately no insecure/trust-all mode.
[[nodiscard]] VerifiedWebSocketEndpoint test_websocket_endpoint(
    std::string connect_host,
    std::string port,
    std::string expected_hostname,
    std::string target,
    std::string ca_file);

class VerifiedWebSocketSession
    : public std::enable_shared_from_this<VerifiedWebSocketSession> {
public:
    [[nodiscard]] static std::shared_ptr<VerifiedWebSocketSession>
    create(
        boost::asio::io_context& io_context,
        VerifiedWebSocketEndpoint endpoint,
        WebSocketSessionCallbacks callbacks,
        WebSocketSessionResult& error) noexcept;

    VerifiedWebSocketSession(const VerifiedWebSocketSession&) = delete;
    VerifiedWebSocketSession& operator=(
        const VerifiedWebSocketSession&) = delete;
    VerifiedWebSocketSession(VerifiedWebSocketSession&&) = delete;
    VerifiedWebSocketSession& operator=(
        VerifiedWebSocketSession&&) = delete;

    ~VerifiedWebSocketSession() noexcept;

    // Both methods are safe to call from outside the I/O thread. Every
    // transition and user callback is serialized through io_context.
    void start();
    void stop();

    [[nodiscard]] bool terminal() const noexcept;

private:
    struct Impl;

    VerifiedWebSocketSession(
        boost::asio::io_context& io_context,
        VerifiedWebSocketEndpoint endpoint,
        WebSocketSessionCallbacks callbacks);

    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const WebSocketSessionStage stage) noexcept {
    switch (stage) {
        case WebSocketSessionStage::none:
            return "none";
        case WebSocketSessionStage::configuration:
            return "configuration";
        case WebSocketSessionStage::resolve:
            return "resolve";
        case WebSocketSessionStage::connect:
            return "connect";
        case WebSocketSessionStage::sni:
            return "sni";
        case WebSocketSessionStage::tls_handshake:
            return "tls_handshake";
        case WebSocketSessionStage::websocket_handshake:
            return "websocket_handshake";
        case WebSocketSessionStage::open:
            return "open";
        case WebSocketSessionStage::websocket_close:
            return "websocket_close";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const WebSocketSessionErrorCode code) noexcept {
    switch (code) {
        case WebSocketSessionErrorCode::none:
            return "none";
        case WebSocketSessionErrorCode::invalid_configuration:
            return "invalid_configuration";
        case WebSocketSessionErrorCode::allocation_failure:
            return "allocation_failure";
        case WebSocketSessionErrorCode::trust_store_failure:
            return "trust_store_failure";
        case WebSocketSessionErrorCode::tls_policy_failure:
            return "tls_policy_failure";
        case WebSocketSessionErrorCode::resolve_failure:
            return "resolve_failure";
        case WebSocketSessionErrorCode::connect_failure:
            return "connect_failure";
        case WebSocketSessionErrorCode::sni_failure:
            return "sni_failure";
        case WebSocketSessionErrorCode::tls_handshake_failure:
            return "tls_handshake_failure";
        case WebSocketSessionErrorCode::websocket_handshake_failure:
            return "websocket_handshake_failure";
        case WebSocketSessionErrorCode::close_failure:
            return "close_failure";
        case WebSocketSessionErrorCode::callback_failure:
            return "callback_failure";
        case WebSocketSessionErrorCode::timeout:
            return "timeout";
        case WebSocketSessionErrorCode::cancelled:
            return "cancelled";
    }
    return "unknown";
}

}  // namespace hft
