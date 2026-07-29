#include "hft/verified_websocket_session.h"

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <openssl/ssl.h>

#include <atomic>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

namespace {

namespace asio = boost::asio;
namespace ssl = asio::ssl;
namespace websocket = boost::beast::websocket;
using Tcp = asio::ip::tcp;

struct ServerOutcome {
    std::string sni{};
    std::string request_target{};
    std::string error{};
    bool websocket_accepted{false};
    bool compression_offered{false};
};

struct ClientOutcome {
    bool opened{false};
    bool terminal_called{false};
    hft::WebSocketSessionResult terminal{};
};

void run_server(
    Tcp::acceptor& acceptor,
    const std::string& certificate_file,
    const std::string& private_key_file,
    const bool expect_websocket,
    ServerOutcome& outcome) noexcept {
    try {
        asio::io_context& io_context =
            static_cast<asio::io_context&>(
                acceptor.get_executor().context());
        ssl::context context{ssl::context::tls_server};
        context.use_certificate_chain_file(certificate_file);
        context.use_private_key_file(
            private_key_file, ssl::context::pem);

        Tcp::socket socket{io_context};
        boost::system::error_code error;
        acceptor.accept(socket, error);
        if (error) {
            outcome.error = "accept: " + error.message();
            return;
        }

        ssl::stream<Tcp::socket> tls_stream{
            std::move(socket), context};
        tls_stream.handshake(ssl::stream_base::server, error);
        const char* const sni = ::SSL_get_servername(
            tls_stream.native_handle(),
            TLSEXT_NAMETYPE_host_name);
        if (sni != nullptr) {
            outcome.sni = sni;
        }
        if (error) {
            if (expect_websocket) {
                outcome.error =
                    "TLS handshake: " + error.message();
            }
            return;
        }
        if (!expect_websocket) {
            return;
        }

        websocket::stream<ssl::stream<Tcp::socket>, false> stream{
            std::move(tls_stream)};
        boost::beast::flat_buffer handshake_buffer;
        boost::beast::http::request<
            boost::beast::http::string_body>
            request;
        boost::beast::http::read(
            stream.next_layer(), handshake_buffer, request, error);
        if (error) {
            outcome.error =
                "HTTP upgrade read: " + error.message();
            return;
        }
        const boost::beast::string_view request_target =
            request.target();
        outcome.request_target.assign(
            request_target.data(), request_target.size());
        outcome.compression_offered =
            request.find(
                boost::beast::http::field::
                    sec_websocket_extensions) != request.end();
        stream.accept(request, error);
        if (error) {
            outcome.error =
                "WebSocket accept: " + error.message();
            return;
        }
        outcome.websocket_accepted = true;

        boost::beast::flat_buffer buffer;
        stream.read(buffer, error);
        if (error != websocket::error::closed) {
            outcome.error =
                "WebSocket close read: " + error.message();
        }
    } catch (const std::exception& error) {
        outcome.error = error.what();
    } catch (...) {
        outcome.error = "unknown server exception";
    }
}

ClientOutcome run_client(
    const hft::VerifiedWebSocketEndpoint& endpoint) {
    asio::io_context io_context;
    ClientOutcome outcome;
    std::shared_ptr<hft::VerifiedWebSocketSession> session;
    hft::WebSocketSessionCallbacks callbacks;
    callbacks.on_open = [&] {
        outcome.opened = true;
        session->stop();
        session->stop();
    };
    callbacks.on_terminal =
        [&](const hft::WebSocketSessionResult result) {
            outcome.terminal_called = true;
            outcome.terminal = result;
        };

    hft::WebSocketSessionResult create_error;
    session = hft::VerifiedWebSocketSession::create(
        io_context,
        endpoint,
        std::move(callbacks),
        0U,
        create_error);
    if (!session) {
        outcome.terminal_called = true;
        outcome.terminal = create_error;
        return outcome;
    }
    session->start();
    io_context.run();
    if (!session->terminal()) {
        outcome.terminal_called = true;
        outcome.terminal = hft::WebSocketSessionResult{
            hft::WebSocketSessionErrorCode::cancelled,
            hft::WebSocketSessionStage::open,
            {}};
    }
    return outcome;
}

bool run_case(
    const std::string& certificate_file,
    const std::string& private_key_file,
    const std::string& ca_file,
    const std::string& expected_hostname,
    const bool trust_test_ca,
    const bool expect_success,
    const std::string& label) {
    asio::io_context server_io;
    Tcp::acceptor acceptor{
        server_io, Tcp::endpoint{asio::ip::address_v4::loopback(), 0U}};
    const std::uint16_t port =
        acceptor.local_endpoint().port();
    ServerOutcome server_outcome;
    std::thread server{
        [&] {
            run_server(
                acceptor,
                certificate_file,
                private_key_file,
                expect_success,
                server_outcome);
        }};

    hft::VerifiedWebSocketEndpoint endpoint =
        hft::test_websocket_endpoint(
            "127.0.0.1",
            std::to_string(port),
            expected_hostname,
            "/stream?streams=btcusdt@trade",
            ca_file);
    if (!trust_test_ca) {
        endpoint.trust_store = hft::TrustStoreKind::system;
        endpoint.test_ca_file.clear();
    }
    const ClientOutcome client_outcome = run_client(endpoint);
    server.join();

    bool success = true;
    const auto fail = [&](const std::string& reason) {
        std::cerr << "FAIL [" << label << "]: " << reason << '\n';
        success = false;
    };
    if (!client_outcome.terminal_called) {
        fail("terminal callback was not called");
    }
    if (expect_success) {
        if (!client_outcome.opened ||
            !client_outcome.terminal.success()) {
            fail(
                "verified handshake/close failed at " +
                std::string{
                    hft::to_string(client_outcome.terminal.stage)} +
                " with " +
                std::string{
                    hft::to_string(client_outcome.terminal.code)});
        }
        if (!server_outcome.websocket_accepted) {
            fail("server did not accept WebSocket handshake");
        }
        if (server_outcome.sni != "localhost") {
            fail("server did not observe exact localhost SNI");
        }
        if (server_outcome.request_target !=
            "/stream?streams=btcusdt@trade") {
            fail("server observed the wrong WebSocket target");
        }
        if (server_outcome.compression_offered) {
            fail("client offered a WebSocket compression extension");
        }
        if (!server_outcome.error.empty()) {
            fail(server_outcome.error);
        }
    } else {
        if (client_outcome.opened) {
            fail("unverified connection reached open state");
        }
        if (client_outcome.terminal.code !=
                hft::WebSocketSessionErrorCode::
                    tls_verification_failure ||
            client_outcome.terminal.stage !=
                hft::WebSocketSessionStage::tls_handshake ||
            !client_outcome.terminal.native_error) {
            fail("TLS verification failure was not classified");
        }
        if (server_outcome.websocket_accepted) {
            fail("server observed WebSocket handshake after TLS failure");
        }
    }
    return success;
}

}  // namespace

int main(const int argc, const char* const* const argv) {
    if (argc != 4) {
        std::cerr
            << "usage: tls_session_integration_test CERT KEY CA\n";
        return 2;
    }
    const std::string certificate_file{argv[1]};
    const std::string private_key_file{argv[2]};
    const std::string ca_file{argv[3]};

    bool success = true;
    success = run_case(
                  certificate_file,
                  private_key_file,
                  ca_file,
                  "localhost",
                  true,
                  true,
                  "verified") &&
              success;
    success = run_case(
                  certificate_file,
                  private_key_file,
                  ca_file,
                  "wrong.example",
                  true,
                  false,
                  "hostname-mismatch") &&
              success;
    success = run_case(
                  certificate_file,
                  private_key_file,
                  ca_file,
                  "localhost",
                  false,
                  false,
                  "untrusted-ca") &&
              success;
    if (success) {
        std::cout << "PASS: verified TLS WebSocket integration\n";
    }
    return success ? 0 : 1;
}
