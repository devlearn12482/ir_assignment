#include "hft/verified_websocket_session.h"

#include "hft/spot_payload_parser.h"

#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <chrono>
#include <atomic>
#include <exception>
#include <new>
#include <string>
#include <utility>

namespace hft {
namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace ssl = asio::ssl;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;
using TlsStream = beast::ssl_stream<beast::tcp_stream>;
using WebSocketStream = websocket::stream<TlsStream, false>;

constexpr std::chrono::seconds kResolveTimeout{5};
constexpr std::chrono::seconds kConnectTimeout{5};
constexpr std::chrono::seconds kTlsTimeout{10};
constexpr std::chrono::seconds kWebSocketTimeout{10};
constexpr std::chrono::seconds kCloseTimeout{2};

[[nodiscard]] bool valid_endpoint_text(
    const std::string_view value) noexcept {
    if (value.empty()) {
        return false;
    }
    for (const char byte : value) {
        const auto unsigned_byte = static_cast<unsigned char>(byte);
        if (byte == '\0' || unsigned_byte < 0x21U ||
            unsigned_byte > 0x7EU) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] boost::system::error_code invalid_argument_error() {
    return boost::system::errc::make_error_code(
        boost::system::errc::invalid_argument);
}

[[nodiscard]] boost::system::error_code openssl_error() {
    return boost::system::error_code{
        static_cast<int>(::ERR_get_error()),
        asio::error::get_ssl_category()};
}

}  // namespace

struct VerifiedWebSocketSession::Impl {
    enum class State : std::uint8_t {
        idle,
        resolving,
        connecting,
        tls_handshaking,
        websocket_handshaking,
        open,
        closing,
        terminal,
    };

    asio::strand<asio::io_context::executor_type> strand;
    ssl::context tls_context{ssl::context::tls_client};
    Tcp::resolver resolver;
    std::unique_ptr<WebSocketStream> websocket{};
    asio::steady_timer deadline;
    VerifiedWebSocketEndpoint endpoint;
    WebSocketSessionCallbacks callbacks;
    State state{State::idle};
    bool start_requested{false};
    std::atomic<bool> terminal_flag{false};

    Impl(
        asio::io_context& io_context,
        VerifiedWebSocketEndpoint configured_endpoint,
        WebSocketSessionCallbacks configured_callbacks)
        : strand{asio::make_strand(io_context)},
          resolver{strand},
          deadline{strand},
          endpoint{std::move(configured_endpoint)},
          callbacks{std::move(configured_callbacks)} {}

    [[nodiscard]] WebSocketSessionResult configure() {
        if (!valid_endpoint_text(endpoint.connect_host) ||
            !valid_endpoint_text(endpoint.port) ||
            !valid_endpoint_text(endpoint.expected_hostname) ||
            endpoint.target.empty() || endpoint.target.front() != '/' ||
            endpoint.target.size() > kMaxCombinedTargetBytes ||
            endpoint.target.find_first_of("\r\n\0", 0U, 3U) !=
                std::string::npos ||
            (endpoint.trust_store ==
                 TrustStoreKind::test_ca_file &&
             endpoint.test_ca_file.empty())) {
            return WebSocketSessionResult{
                WebSocketSessionErrorCode::invalid_configuration,
                WebSocketSessionStage::configuration,
                invalid_argument_error()};
        }

        boost::system::error_code error;
        tls_context.set_options(
            ssl::context::default_workarounds |
                ssl::context::no_sslv2 | ssl::context::no_sslv3 |
                ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1,
            error);
        if (error) {
            return WebSocketSessionResult{
                WebSocketSessionErrorCode::tls_policy_failure,
                WebSocketSessionStage::configuration,
                error};
        }
        tls_context.set_verify_mode(ssl::verify_peer, error);
        if (error) {
            return WebSocketSessionResult{
                WebSocketSessionErrorCode::tls_policy_failure,
                WebSocketSessionStage::configuration,
                error};
        }
        if (::SSL_CTX_set_min_proto_version(
                tls_context.native_handle(), TLS1_2_VERSION) != 1) {
            return WebSocketSessionResult{
                WebSocketSessionErrorCode::tls_policy_failure,
                WebSocketSessionStage::configuration,
                openssl_error()};
        }
        if (endpoint.trust_store == TrustStoreKind::system) {
            tls_context.set_default_verify_paths(error);
        } else {
            tls_context.load_verify_file(
                endpoint.test_ca_file, error);
        }
        if (error) {
            return WebSocketSessionResult{
                WebSocketSessionErrorCode::trust_store_failure,
                WebSocketSessionStage::configuration,
                error};
        }
        websocket =
            std::make_unique<WebSocketStream>(strand, tls_context);
        return {};
    }

    [[nodiscard]] static constexpr WebSocketSessionStage
    stage_for_state(const State current_state) noexcept {
        switch (current_state) {
            case State::idle:
                return WebSocketSessionStage::none;
            case State::resolving:
                return WebSocketSessionStage::resolve;
            case State::connecting:
                return WebSocketSessionStage::connect;
            case State::tls_handshaking:
                return WebSocketSessionStage::tls_handshake;
            case State::websocket_handshaking:
                return WebSocketSessionStage::websocket_handshake;
            case State::open:
                return WebSocketSessionStage::open;
            case State::closing:
                return WebSocketSessionStage::websocket_close;
            case State::terminal:
                return WebSocketSessionStage::none;
        }
        return WebSocketSessionStage::none;
    }

    void arm_deadline(
        const std::chrono::seconds timeout,
        const State expected_state,
        const WebSocketSessionStage stage,
        std::shared_ptr<VerifiedWebSocketSession> owner) {
        deadline.expires_after(timeout);
        deadline.async_wait(
            [owner = std::move(owner), expected_state, stage](
                const boost::system::error_code& error) {
                Impl& self = *owner->impl_;
                if (error == asio::error::operation_aborted ||
                    self.state != expected_state ||
                    self.state == State::terminal) {
                    return;
                }
                self.finish(
                    WebSocketSessionResult{
                        WebSocketSessionErrorCode::timeout,
                        stage,
                        asio::error::timed_out});
            });
    }

    void cancel_deadline() noexcept {
        boost::system::error_code ignored;
        deadline.cancel(ignored);
    }

    void close_lowest_layer() noexcept {
        if (!websocket) {
            return;
        }
        boost::system::error_code ignored;
        beast::get_lowest_layer(*websocket).socket().cancel(ignored);
        beast::get_lowest_layer(*websocket).socket().shutdown(
            Tcp::socket::shutdown_both, ignored);
        beast::get_lowest_layer(*websocket).socket().close(ignored);
    }

    void finish(WebSocketSessionResult result) {
        if (state == State::terminal) {
            return;
        }
        state = State::terminal;
        terminal_flag.store(true, std::memory_order_release);
        cancel_deadline();
        resolver.cancel();
        close_lowest_layer();
        auto terminal_callback = std::move(callbacks.on_terminal);
        callbacks = {};
        if (terminal_callback) {
            try {
                terminal_callback(result);
            } catch (...) {
                // Terminal state and resource teardown are already complete.
            }
        }
    }

    void start(std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state != State::idle || start_requested) {
            return;
        }
        start_requested = true;
        state = State::resolving;
        arm_deadline(
            kResolveTimeout,
            State::resolving,
            WebSocketSessionStage::resolve,
            owner);
        resolver.async_resolve(
            endpoint.connect_host,
            endpoint.port,
            [owner = std::move(owner)](
                const boost::system::error_code& error,
                const Tcp::resolver::results_type& results) {
                Impl& self = *owner->impl_;
                if (self.state != State::resolving) {
                    return;
                }
                self.cancel_deadline();
                if (error) {
                    self.finish(WebSocketSessionResult{
                        WebSocketSessionErrorCode::resolve_failure,
                        WebSocketSessionStage::resolve,
                        error});
                    return;
                }
                self.state = State::connecting;
                self.arm_deadline(
                    kConnectTimeout,
                    State::connecting,
                    WebSocketSessionStage::connect,
                    owner);
                beast::get_lowest_layer(*self.websocket)
                    .async_connect(
                        results,
                        [owner = std::move(owner)](
                            const boost::system::error_code&
                                connect_error,
                            const Tcp::resolver::results_type::
                                endpoint_type&) {
                            owner->impl_->on_connect(
                                connect_error, owner);
                        });
            });
    }

    void on_connect(
        const boost::system::error_code& error,
        std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state != State::connecting) {
            return;
        }
        cancel_deadline();
        if (error) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::connect_failure,
                WebSocketSessionStage::connect,
                error});
            return;
        }

        ::ERR_clear_error();
        if (::SSL_set_tlsext_host_name(
                websocket->next_layer().native_handle(),
                endpoint.expected_hostname.c_str()) != 1) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::sni_failure,
                WebSocketSessionStage::sni,
                openssl_error()});
            return;
        }
        websocket->next_layer().set_verify_callback(
            ssl::host_name_verification(
                endpoint.expected_hostname));
        state = State::tls_handshaking;
        arm_deadline(
            kTlsTimeout,
            State::tls_handshaking,
            WebSocketSessionStage::tls_handshake,
            owner);
        websocket->next_layer().async_handshake(
            ssl::stream_base::client,
            [owner = std::move(owner)](
                const boost::system::error_code& handshake_error) {
                owner->impl_->on_tls_handshake(
                    handshake_error, owner);
            });
    }

    void on_tls_handshake(
        const boost::system::error_code& error,
        std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state != State::tls_handshaking) {
            return;
        }
        cancel_deadline();
        if (error) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::tls_handshake_failure,
                WebSocketSessionStage::tls_handshake,
                error});
            return;
        }

        websocket->read_message_max(kMaxPayloadBytes);
        websocket->set_option(
            websocket::stream_base::timeout::suggested(
                beast::role_type::client));
        websocket->set_option(websocket::stream_base::decorator(
            [](websocket::request_type& request) {
                request.set(
                    boost::beast::http::field::user_agent,
                    "binance_capture/0.1");
            }));

        std::string host_header = endpoint.expected_hostname;
        if (endpoint.port != "443") {
            host_header.push_back(':');
            host_header.append(endpoint.port);
        }
        state = State::websocket_handshaking;
        arm_deadline(
            kWebSocketTimeout,
            State::websocket_handshaking,
            WebSocketSessionStage::websocket_handshake,
            owner);
        websocket->async_handshake(
            host_header,
            endpoint.target,
            [owner = std::move(owner)](
                const boost::system::error_code& handshake_error) {
                Impl& self = *owner->impl_;
                if (self.state != State::websocket_handshaking) {
                    return;
                }
                self.cancel_deadline();
                if (handshake_error) {
                    self.finish(WebSocketSessionResult{
                        WebSocketSessionErrorCode::
                            websocket_handshake_failure,
                        WebSocketSessionStage::websocket_handshake,
                        handshake_error});
                    return;
                }
                self.state = State::open;
                if (self.callbacks.on_open) {
                    try {
                        self.callbacks.on_open();
                    } catch (...) {
                        self.finish(WebSocketSessionResult{
                            WebSocketSessionErrorCode::
                                callback_failure,
                            WebSocketSessionStage::open,
                            {}});
                    }
                }
            });
    }

    void stop(std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state == State::terminal) {
            return;
        }
        if (state != State::open) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::cancelled,
                stage_for_state(state),
                asio::error::operation_aborted});
            return;
        }

        state = State::closing;
        arm_deadline(
            kCloseTimeout,
            State::closing,
            WebSocketSessionStage::websocket_close,
            owner);
        websocket->async_close(
            websocket::close_code::normal,
            [owner = std::move(owner)](
                const boost::system::error_code& error) {
                Impl& self = *owner->impl_;
                if (self.state != State::closing) {
                    return;
                }
                self.cancel_deadline();
                if (error && error != websocket::error::closed) {
                    self.finish(WebSocketSessionResult{
                        WebSocketSessionErrorCode::close_failure,
                        WebSocketSessionStage::websocket_close,
                        error});
                    return;
                }
                self.finish({});
            });
    }
};

VerifiedWebSocketEndpoint production_websocket_endpoint(
    const LiveSubscription& subscription) {
    const VenueEndpoint& endpoint = subscription.endpoint();
    VerifiedWebSocketEndpoint result;
    result.connect_host = endpoint.host;
    result.port = endpoint.port;
    result.expected_hostname = endpoint.host;
    result.target = subscription.target();
    result.trust_store = TrustStoreKind::system;
    return result;
}

VerifiedWebSocketEndpoint test_websocket_endpoint(
    std::string connect_host,
    std::string port,
    std::string expected_hostname,
    std::string target,
    std::string ca_file) {
    VerifiedWebSocketEndpoint result;
    result.connect_host = std::move(connect_host);
    result.port = std::move(port);
    result.expected_hostname = std::move(expected_hostname);
    result.target = std::move(target);
    result.trust_store = TrustStoreKind::test_ca_file;
    result.test_ca_file = std::move(ca_file);
    return result;
}

std::shared_ptr<VerifiedWebSocketSession>
VerifiedWebSocketSession::create(
    asio::io_context& io_context,
    VerifiedWebSocketEndpoint endpoint,
    WebSocketSessionCallbacks callbacks,
    WebSocketSessionResult& error) noexcept {
    error = {};
    try {
        std::shared_ptr<VerifiedWebSocketSession> session{
            new VerifiedWebSocketSession{
                io_context,
                std::move(endpoint),
                std::move(callbacks)}};
        error = session->impl_->configure();
        if (!error.success()) {
            return nullptr;
        }
        return session;
    } catch (const std::bad_alloc&) {
        error = WebSocketSessionResult{
            WebSocketSessionErrorCode::allocation_failure,
            WebSocketSessionStage::configuration,
            {}};
        return nullptr;
    } catch (const std::exception&) {
        error = WebSocketSessionResult{
            WebSocketSessionErrorCode::tls_policy_failure,
            WebSocketSessionStage::configuration,
            {}};
        return nullptr;
    }
}

VerifiedWebSocketSession::VerifiedWebSocketSession(
    asio::io_context& io_context,
    VerifiedWebSocketEndpoint endpoint,
    WebSocketSessionCallbacks callbacks)
    : impl_{std::make_unique<Impl>(
          io_context,
          std::move(endpoint),
          std::move(callbacks))} {}

VerifiedWebSocketSession::~VerifiedWebSocketSession() noexcept = default;

void VerifiedWebSocketSession::start() {
    const std::shared_ptr<VerifiedWebSocketSession> owner =
        shared_from_this();
    asio::dispatch(
        impl_->strand,
        [owner] { owner->impl_->start(owner); });
}

void VerifiedWebSocketSession::stop() {
    const std::shared_ptr<VerifiedWebSocketSession> owner =
        shared_from_this();
    asio::dispatch(
        impl_->strand,
        [owner] { owner->impl_->stop(owner); });
}

bool VerifiedWebSocketSession::terminal() const noexcept {
    return impl_->terminal_flag.load(std::memory_order_acquire);
}

}  // namespace hft
