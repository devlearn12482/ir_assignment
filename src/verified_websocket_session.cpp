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
#include <boost/beast/core/flat_static_buffer.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
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
    beast::flat_static_buffer<kMaxPayloadBytes> read_buffer;
    asio::steady_timer deadline;
    VerifiedWebSocketEndpoint endpoint;
    WebSocketSessionCallbacks callbacks;
    State state{State::idle};
    bool start_requested{false};
    bool read_in_progress{false};
    bool in_control_callback{false};
    bool stop_requested_during_control{false};
    bool control_callback_failed{false};
    std::uint64_t connection_epoch{0};
    std::uint64_t connection_sequence{0};
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
        websocket->control_callback(
            [this](
                const websocket::frame_type frame_type,
                const beast::string_view payload) {
                on_control_frame(frame_type, payload);
            });
        return {};
    }

    [[nodiscard]] WebSocketSessionStage active_stage() const noexcept {
        switch (state) {
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
                return read_in_progress
                           ? WebSocketSessionStage::read
                           : WebSocketSessionStage::open;
            case State::closing:
                return WebSocketSessionStage::websocket_close;
            case State::terminal:
                return WebSocketSessionStage::none;
        }
        return WebSocketSessionStage::none;
    }

    void on_control_frame(
        const websocket::frame_type frame_type,
        const beast::string_view payload) noexcept {
        WebSocketControlKind kind;
        switch (frame_type) {
            case websocket::frame_type::ping:
                kind = WebSocketControlKind::ping;
                break;
            case websocket::frame_type::pong:
                kind = WebSocketControlKind::pong;
                break;
            case websocket::frame_type::close:
                kind = WebSocketControlKind::close;
                break;
            default:
                return;
        }
        if (!callbacks.on_control) {
            return;
        }
        in_control_callback = true;
        try {
            callbacks.on_control(WebSocketControlFrame{
                kind,
                std::string_view{payload.data(), payload.size()}});
        } catch (...) {
            control_callback_failed = true;
        }
        in_control_callback = false;
        if ((control_callback_failed ||
             stop_requested_during_control) &&
            websocket) {
            boost::system::error_code ignored;
            beast::get_lowest_layer(*websocket)
                .socket()
                .cancel(ignored);
        }
    }

    [[nodiscard]] static bool capture_receive_timestamp(
        CsvTimestamp& timestamp) noexcept {
        const auto elapsed =
            std::chrono::system_clock::now().time_since_epoch();
        const auto seconds =
            std::chrono::duration_cast<std::chrono::seconds>(
                elapsed);
        const auto nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                elapsed - seconds);
        if (seconds.count() < 0 || nanoseconds.count() < 0 ||
            nanoseconds.count() >= 1'000'000'000) {
            return false;
        }
        timestamp.seconds =
            static_cast<std::uint64_t>(seconds.count());
        timestamp.nanoseconds =
            static_cast<std::uint32_t>(nanoseconds.count());
        return true;
    }

    void clear_read_buffer() noexcept {
        read_buffer.consume(read_buffer.size());
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
        result.last_connection_sequence = connection_sequence;
        state = State::terminal;
        terminal_flag.store(true, std::memory_order_release);
        cancel_deadline();
        resolver.cancel();
        close_lowest_layer();
        if (!read_in_progress) {
            clear_read_buffer();
        }
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

    void issue_read(
        std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state != State::open || read_in_progress) {
            return;
        }
        read_in_progress = true;
        websocket->async_read(
            read_buffer,
            [owner = std::move(owner)](
                const boost::system::error_code& error,
                const std::size_t bytes_transferred) {
                owner->impl_->on_read(
                    error, bytes_transferred, owner);
            });
    }

    void on_read(
        const boost::system::error_code& error,
        const std::size_t bytes_transferred,
        std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (!read_in_progress) {
            return;
        }
        read_in_progress = false;
        if (state != State::open) {
            clear_read_buffer();
            return;
        }

        if (control_callback_failed) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::callback_failure,
                WebSocketSessionStage::read,
                error});
            return;
        }
        if (stop_requested_during_control) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::cancelled,
                WebSocketSessionStage::read,
                error});
            return;
        }
        if (error) {
            const bool had_incomplete_bytes =
                read_buffer.size() != 0U;
            clear_read_buffer();
            if (error == websocket::error::message_too_big ||
                error == websocket::error::buffer_overflow) {
                finish(WebSocketSessionResult{
                    WebSocketSessionErrorCode::message_too_big,
                    WebSocketSessionStage::read,
                    error});
                return;
            }
            if (error == websocket::error::closed) {
                finish(WebSocketSessionResult{
                    WebSocketSessionErrorCode::remote_close,
                    WebSocketSessionStage::read,
                    error});
                return;
            }
            finish(WebSocketSessionResult{
                had_incomplete_bytes
                    ? WebSocketSessionErrorCode::
                          incomplete_message
                    : WebSocketSessionErrorCode::read_failure,
                WebSocketSessionStage::read,
                error});
            return;
        }
        if (bytes_transferred != read_buffer.size()) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::read_failure,
                WebSocketSessionStage::read,
                invalid_argument_error()});
            return;
        }
        if (connection_sequence ==
            std::numeric_limits<std::uint64_t>::max()) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::sequence_overflow,
                WebSocketSessionStage::read,
                {}});
            return;
        }
        ++connection_sequence;

        CsvTimestamp timestamp;
        if (!capture_receive_timestamp(timestamp)) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::timestamp_failure,
                WebSocketSessionStage::read,
                {}});
            return;
        }
        if (!websocket->got_text()) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::binary_message,
                WebSocketSessionStage::read,
                {}});
            return;
        }

        const asio::const_buffer payload_buffer =
            beast::buffers_front(read_buffer.data());
        const WebSocketTextMessage message{
            timestamp,
            connection_epoch,
            connection_sequence,
            std::string_view{
                static_cast<const char*>(payload_buffer.data()),
                payload_buffer.size()}};
        if (!callbacks.on_text_message) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::callback_failure,
                WebSocketSessionStage::read,
                {}});
            return;
        }
        try {
            callbacks.on_text_message(message);
        } catch (...) {
            clear_read_buffer();
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::callback_failure,
                WebSocketSessionStage::read,
                {}});
            return;
        }
        clear_read_buffer();
        if (state == State::open) {
            issue_read(std::move(owner));
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
                if (self.state == State::open) {
                    self.issue_read(std::move(owner));
                }
            });
    }

    void stop(std::shared_ptr<VerifiedWebSocketSession> owner) {
        if (state == State::terminal) {
            return;
        }
        if (state == State::closing) {
            return;
        }
        if (in_control_callback) {
            stop_requested_during_control = true;
            return;
        }
        if (state != State::open) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::cancelled,
                active_stage(),
                asio::error::operation_aborted});
            return;
        }
        if (read_in_progress) {
            finish(WebSocketSessionResult{
                WebSocketSessionErrorCode::cancelled,
                WebSocketSessionStage::read,
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
                if (self.control_callback_failed) {
                    self.finish(WebSocketSessionResult{
                        WebSocketSessionErrorCode::
                            callback_failure,
                        WebSocketSessionStage::websocket_close,
                        error});
                    return;
                }
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
