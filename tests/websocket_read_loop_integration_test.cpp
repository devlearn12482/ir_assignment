#include "hft/spot_payload_parser.h"
#include "hft/verified_websocket_session.h"

#include <boost/asio/buffer.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace ssl = asio::ssl;
namespace websocket = beast::websocket;
using Tcp = asio::ip::tcp;

constexpr char kFragmentedPayload[]{
    R"({"stream":"btcusdt@trade","data":{"p":"1.25"}})"};
constexpr char kPingPayload[]{"probe-42"};

enum class Scenario : std::uint8_t {
    fragmented_text_with_ping,
    sequential_text_messages,
    exact_limit_text,
    binary_message,
    oversized_message,
    incomplete_fragment,
    remote_close,
    active_read_stop,
    text_callback_failure,
    pause_resume,
    paused_stop,
};

struct ServerOutcome {
    std::string error{};
    std::string pong_payload{};
    std::uint32_t pong_count{0};
    bool websocket_accepted{false};
};

struct ClientOutcome {
    bool opened{false};
    bool terminal_called{false};
    std::uint32_t text_messages{0};
    std::uint32_t ping_controls{0};
    std::uint32_t pong_controls{0};
    std::uint32_t close_controls{0};
    std::string ping_payload{};
    std::string text_payload{};
    std::uint64_t connection_epoch{0};
    std::uint64_t first_connection_sequence{0};
    std::uint64_t connection_sequence{0};
    hft::CsvTimestamp timestamp{};
    bool exact_limit_payload_valid{false};
    std::int64_t pause_delay_milliseconds{0};
    hft::WebSocketSessionResult terminal{};
};

template <typename Stream>
void wait_for_peer_close(
    Stream& stream,
    ServerOutcome& outcome) {
    beast::flat_buffer buffer;
    boost::system::error_code error;
    stream.read(buffer, error);
    if (error != websocket::error::closed) {
        outcome.error =
            "waiting for peer close: " + error.message();
    }
}

void run_server(
    Tcp::acceptor& acceptor,
    const std::string& certificate_file,
    const std::string& private_key_file,
    const Scenario scenario,
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
        tls_stream.handshake(
            ssl::stream_base::server, error);
        if (error) {
            outcome.error =
                "TLS handshake: " + error.message();
            return;
        }
        websocket::stream<ssl::stream<Tcp::socket>, false> stream{
            std::move(tls_stream)};
        stream.accept(error);
        if (error) {
            outcome.error =
                "WebSocket accept: " + error.message();
            return;
        }
        outcome.websocket_accepted = true;
        stream.control_callback(
            [&](const websocket::frame_type kind,
                const beast::string_view payload) {
                if (kind == websocket::frame_type::pong) {
                    ++outcome.pong_count;
                    outcome.pong_payload.assign(
                        payload.data(), payload.size());
                }
            });

        switch (scenario) {
            case Scenario::fragmented_text_with_ping: {
                stream.ping(
                    websocket::ping_data{kPingPayload}, error);
                if (error) {
                    outcome.error =
                        "ping write: " + error.message();
                    return;
                }
                constexpr std::size_t split{
                    (sizeof(kFragmentedPayload) - 1U) / 2U};
                stream.text(true);
                const std::size_t first_written =
                    stream.write_some(
                        false,
                        asio::buffer(
                            kFragmentedPayload, split),
                        error);
                if (error || first_written != split) {
                    outcome.error =
                        "first fragment write: " +
                        error.message();
                    return;
                }
                constexpr std::size_t remaining{
                    sizeof(kFragmentedPayload) - 1U - split};
                const std::size_t second_written =
                    stream.write_some(
                        true,
                        asio::buffer(
                            kFragmentedPayload + split,
                            remaining),
                        error);
                if (error || second_written != remaining) {
                    outcome.error =
                        "final fragment write: " +
                        error.message();
                    return;
                }
                wait_for_peer_close(stream, outcome);
                return;
            }
            case Scenario::sequential_text_messages:
            case Scenario::pause_resume: {
                stream.text(true);
                stream.write(
                    asio::buffer("one", 3U), error);
                if (error) {
                    outcome.error =
                        "first message write: " +
                        error.message();
                    return;
                }
                stream.write(
                    asio::buffer("two", 3U), error);
                if (error) {
                    outcome.error =
                        "second message write: " +
                        error.message();
                    return;
                }
                wait_for_peer_close(stream, outcome);
                return;
            }
            case Scenario::exact_limit_text: {
                const std::string payload(
                    hft::kMaxPayloadBytes, 'x');
                stream.text(true);
                stream.write(asio::buffer(payload), error);
                if (error) {
                    outcome.error =
                        "exact-limit write: " +
                        error.message();
                    return;
                }
                wait_for_peer_close(stream, outcome);
                return;
            }
            case Scenario::binary_message: {
                constexpr char payload[]{"binary"};
                stream.binary(true);
                stream.write(asio::buffer(payload), error);
                if (error) {
                    outcome.error =
                        "binary write: " + error.message();
                }
                return;
            }
            case Scenario::oversized_message: {
                const std::string payload(
                    hft::kMaxPayloadBytes + 1U, 'y');
                stream.text(true);
                stream.write(asio::buffer(payload), error);
                return;
            }
            case Scenario::incomplete_fragment: {
                constexpr char payload[]{"partial-message"};
                stream.text(true);
                static_cast<void>(stream.write_some(
                    false, asio::buffer(payload), error));
                if (error) {
                    outcome.error =
                        "partial fragment write: " +
                        error.message();
                    return;
                }
                beast::get_lowest_layer(stream).shutdown(
                    Tcp::socket::shutdown_both, error);
                beast::get_lowest_layer(stream).close(error);
                return;
            }
            case Scenario::remote_close:
                stream.close(
                    websocket::close_code::normal, error);
                if (error) {
                    outcome.error =
                        "remote close: " + error.message();
                }
                return;
            case Scenario::active_read_stop: {
                beast::flat_buffer buffer;
                stream.read(buffer, error);
                return;
            }
            case Scenario::text_callback_failure:
                stream.text(true);
                stream.write(
                    asio::buffer("throw", 5U), error);
                if (error) {
                    outcome.error =
                        "callback-failure write: " +
                        error.message();
                }
                return;
            case Scenario::paused_stop:
                stream.text(true);
                stream.write(
                    asio::buffer("pause", 5U), error);
                if (error) {
                    outcome.error =
                        "paused-stop write: " +
                        error.message();
                    return;
                }
                wait_for_peer_close(stream, outcome);
                return;
        }
    } catch (const std::exception& error) {
        outcome.error = error.what();
    } catch (...) {
        outcome.error = "unknown server exception";
    }
}

ClientOutcome run_client(
    const std::uint16_t port,
    const std::string& ca_file,
    const Scenario scenario) {
    asio::io_context io_context;
    ClientOutcome outcome;
    std::shared_ptr<hft::VerifiedWebSocketSession> session;
    asio::steady_timer resume_timer{io_context};
    std::chrono::steady_clock::time_point pause_started{};
    hft::WebSocketSessionCallbacks callbacks;
    callbacks.on_open = [&] {
        outcome.opened = true;
        if (scenario == Scenario::active_read_stop) {
            asio::post(
                io_context,
                [&] { session->stop(); });
        }
    };
    callbacks.on_text_message =
        [&](const hft::WebSocketTextMessage& message) {
            ++outcome.text_messages;
            if (outcome.text_messages == 1U) {
                outcome.first_connection_sequence =
                    message.connection_sequence;
            }
            outcome.connection_epoch =
                message.connection_epoch;
            outcome.connection_sequence =
                message.connection_sequence;
            outcome.timestamp = message.receive_timestamp;
            if (scenario ==
                Scenario::fragmented_text_with_ping) {
                outcome.text_payload.assign(
                    message.payload.data(),
                    message.payload.size());
            } else if (
                scenario ==
                    Scenario::sequential_text_messages ||
                scenario == Scenario::pause_resume) {
                if (!outcome.text_payload.empty()) {
                    outcome.text_payload.push_back('/');
                }
                outcome.text_payload.append(
                    message.payload.data(),
                    message.payload.size());
            } else if (
                scenario == Scenario::exact_limit_text) {
                outcome.exact_limit_payload_valid =
                    message.payload.size() ==
                        hft::kMaxPayloadBytes &&
                    message.payload.find_first_not_of('x') ==
                        std::string_view::npos;
            }
            if (scenario ==
                Scenario::text_callback_failure) {
                throw std::runtime_error{
                    "injected text callback failure"};
            }
            if (scenario == Scenario::pause_resume &&
                outcome.text_messages == 1U) {
                pause_started =
                    std::chrono::steady_clock::now();
                session->pause_reads();
                session->pause_reads();
                resume_timer.expires_after(
                    std::chrono::milliseconds{100});
                resume_timer.async_wait(
                    [&](const boost::system::error_code& error) {
                        if (!error) {
                            session->resume_reads();
                            session->resume_reads();
                        }
                    });
                return;
            }
            if (scenario == Scenario::pause_resume &&
                outcome.text_messages == 2U) {
                outcome.pause_delay_milliseconds =
                    std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() -
                        pause_started)
                        .count();
            }
            if (scenario == Scenario::paused_stop) {
                session->pause_reads();
                session->pause_reads();
                asio::post(
                    io_context,
                    [&] { session->stop(); });
                return;
            }
            if (scenario !=
                    Scenario::sequential_text_messages ||
                outcome.text_messages == 2U) {
                session->stop();
            }
        };
    callbacks.on_control =
        [&](const hft::WebSocketControlFrame& frame) {
            switch (frame.kind) {
                case hft::WebSocketControlKind::ping:
                    ++outcome.ping_controls;
                    outcome.ping_payload.assign(
                        frame.payload.data(),
                        frame.payload.size());
                    break;
                case hft::WebSocketControlKind::pong:
                    ++outcome.pong_controls;
                    break;
                case hft::WebSocketControlKind::close:
                    ++outcome.close_controls;
                    break;
            }
        };
    callbacks.on_terminal =
        [&](const hft::WebSocketSessionResult result) {
            outcome.terminal_called = true;
            outcome.terminal = result;
        };

    hft::WebSocketSessionResult create_error;
    session = hft::VerifiedWebSocketSession::create(
        io_context,
        hft::test_websocket_endpoint(
            "127.0.0.1",
            std::to_string(port),
            "localhost",
            "/stream?streams=btcusdt@trade",
            ca_file),
        std::move(callbacks),
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
            hft::WebSocketSessionStage::read,
            {}};
    }
    return outcome;
}

bool valid_message_metadata(const ClientOutcome& outcome) {
    return outcome.connection_epoch == 0U &&
           outcome.connection_sequence == 1U &&
           outcome.timestamp.seconds != 0U &&
           outcome.timestamp.nanoseconds < 1'000'000'000U;
}

bool run_case(
    const std::string& certificate_file,
    const std::string& private_key_file,
    const std::string& ca_file,
    const Scenario scenario,
    const std::string& label) {
    asio::io_context server_io;
    Tcp::acceptor acceptor{
        server_io,
        Tcp::endpoint{asio::ip::address_v4::loopback(), 0U}};
    const std::uint16_t port =
        acceptor.local_endpoint().port();
    ServerOutcome server_outcome;
    std::thread server{
        [&] {
            run_server(
                acceptor,
                certificate_file,
                private_key_file,
                scenario,
                server_outcome);
        }};
    const ClientOutcome client_outcome =
        run_client(port, ca_file, scenario);
    server.join();

    bool success = true;
    const auto fail = [&](const std::string& reason) {
        std::cerr << "FAIL [" << label << "]: "
                  << reason << '\n';
        success = false;
    };
    if (!server_outcome.websocket_accepted) {
        fail("server did not accept WebSocket");
    }
    if (!server_outcome.error.empty()) {
        fail(server_outcome.error);
    }
    if (!client_outcome.opened) {
        fail("client did not reach open state");
    }
    if (!client_outcome.terminal_called) {
        fail("terminal callback was not called");
    }

    switch (scenario) {
        case Scenario::fragmented_text_with_ping:
            if (!client_outcome.terminal.success() ||
                client_outcome.text_messages != 1U ||
                client_outcome.text_payload !=
                    kFragmentedPayload ||
                !valid_message_metadata(client_outcome)) {
                fail("fragmented message was not delivered once");
            }
            if (client_outcome.ping_controls != 1U ||
                client_outcome.ping_payload != kPingPayload ||
                client_outcome.pong_controls != 0U ||
                server_outcome.pong_count != 1U ||
                server_outcome.pong_payload != kPingPayload) {
                fail("ping was not observed and answered exactly once");
            }
            break;
        case Scenario::sequential_text_messages:
            if (!client_outcome.terminal.success() ||
                client_outcome.text_messages != 2U ||
                client_outcome.text_payload != "one/two" ||
                client_outcome.first_connection_sequence != 1U ||
                client_outcome.connection_sequence != 2U ||
                client_outcome.connection_epoch != 0U ||
                client_outcome.terminal.
                        last_connection_sequence != 2U) {
                fail("sequential messages lost order or sequence");
            }
            break;
        case Scenario::exact_limit_text:
            if (!client_outcome.terminal.success() ||
                client_outcome.text_messages != 1U ||
                !client_outcome.exact_limit_payload_valid ||
                !valid_message_metadata(client_outcome)) {
                fail("exact-limit text message was not accepted");
            }
            break;
        case Scenario::binary_message:
            if (client_outcome.text_messages != 0U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::
                        binary_message ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 1U) {
                fail("binary message policy was not enforced");
            }
            break;
        case Scenario::oversized_message:
            if (client_outcome.text_messages != 0U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::
                        message_too_big ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 0U) {
                fail(
                    "one-over-limit result was " +
                    std::string{hft::to_string(
                        client_outcome.terminal.code)} +
                    "/" +
                    std::string{hft::to_string(
                        client_outcome.terminal.stage)} +
                    ", native=" +
                    client_outcome.terminal.native_error.message() +
                    ", seq=" +
                    std::to_string(
                        client_outcome.terminal.
                            last_connection_sequence));
            }
            break;
        case Scenario::incomplete_fragment:
            if (client_outcome.text_messages != 0U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::
                        incomplete_message ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 0U) {
                fail("incomplete message was not discarded");
            }
            break;
        case Scenario::remote_close:
            if (client_outcome.text_messages != 0U ||
                client_outcome.close_controls != 1U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::
                        remote_close ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 0U) {
                fail("remote close was not observed and classified");
            }
            break;
        case Scenario::active_read_stop:
            if (client_outcome.text_messages != 0U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::cancelled ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 0U) {
                fail("active read did not cancel deterministically");
            }
            break;
        case Scenario::text_callback_failure:
            if (client_outcome.text_messages != 1U ||
                client_outcome.terminal.code !=
                    hft::WebSocketSessionErrorCode::
                        callback_failure ||
                client_outcome.terminal.stage !=
                    hft::WebSocketSessionStage::read ||
                client_outcome.terminal.
                        last_connection_sequence != 1U) {
                fail("text callback exception escaped the session");
            }
            break;
        case Scenario::pause_resume:
            if (!client_outcome.terminal.success() ||
                client_outcome.text_messages != 2U ||
                client_outcome.text_payload != "one/two" ||
                client_outcome.pause_delay_milliseconds < 75 ||
                client_outcome.terminal.
                        last_connection_sequence != 2U) {
                fail(
                    "paused read resumed early, duplicated, or lost data");
            }
            break;
        case Scenario::paused_stop:
            if (!client_outcome.terminal.success() ||
                client_outcome.text_messages != 1U ||
                client_outcome.connection_sequence != 1U ||
                client_outcome.terminal.
                        last_connection_sequence != 1U) {
                fail("stop while paused did not close cleanly");
            }
            break;
    }
    return success;
}

}  // namespace

int main(const int argc, const char* const* const argv) {
    if (argc != 4) {
        std::cerr
            << "usage: websocket_read_loop_integration_test "
               "CERT KEY CA\n";
        return 2;
    }
    const std::string certificate_file{argv[1]};
    const std::string private_key_file{argv[2]};
    const std::string ca_file{argv[3]};

    bool success = true;
    const auto run =
        [&](const Scenario scenario, const std::string& label) {
            success = run_case(
                          certificate_file,
                          private_key_file,
                          ca_file,
                          scenario,
                          label) &&
                      success;
        };
    run(
        Scenario::fragmented_text_with_ping,
        "fragmented-text-ping");
    run(
        Scenario::sequential_text_messages,
        "sequential-text-messages");
    run(Scenario::exact_limit_text, "exact-limit-text");
    run(Scenario::binary_message, "binary-message");
    run(Scenario::oversized_message, "oversized-message");
    run(Scenario::incomplete_fragment, "incomplete-fragment");
    run(Scenario::remote_close, "remote-close");
    run(Scenario::active_read_stop, "active-read-stop");
    run(
        Scenario::text_callback_failure,
        "text-callback-failure");
    run(Scenario::pause_resume, "pause-resume");
    run(Scenario::paused_stop, "paused-stop");
    if (success) {
        std::cout
            << "PASS: bounded WebSocket read loop integration\n";
    }
    return success ? 0 : 1;
}
