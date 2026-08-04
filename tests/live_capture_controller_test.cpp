#include "test_framework.h"

#include "hft/csv_output_set.h"
#include "hft/live_capture_controller.h"
#include "hft/live_subscription.h"

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>

namespace hft {

struct LiveCaptureControllerTestAccess {
    static void on_session_terminal(
        LiveCaptureController& controller,
        const std::uint64_t generation,
        const WebSocketSessionResult result) noexcept {
        controller.on_session_terminal(generation, result);
    }
};

}  // namespace hft

namespace hft::test {
namespace {

class ControllerTemporaryDirectory {
public:
    ControllerTemporaryDirectory() {
        static std::uint64_t sequence{0U};
        path_ = std::filesystem::temp_directory_path() /
            ("hft_controller_unit_" +
             std::to_string(sequence++));
        std::filesystem::create_directory(path_);
    }

    ~ControllerTemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path()
        const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_{};
};

void test_output_venue_must_match_subscription(
    Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{
        "BTCUSDT"};
    SubscriptionError subscription_error;
    std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            subscription_error);
    ControllerTemporaryDirectory parent;
    OutputSetOpenError output_error;
    std::unique_ptr<CsvOutputSet> output =
        CsvOutputSet::open_live(
            (parent.path() / "capture").string(),
            PayloadVenue::usdm,
            symbols.data(),
            symbols.size(),
            "2026-07-28",
            output_error);
    boost::asio::io_context io_context;
    LiveCaptureCreateError create_error;
    const std::shared_ptr<LiveCaptureController> controller =
        LiveCaptureController::create(
            io_context,
            std::move(subscription),
            std::move(output),
            {},
            {},
            create_error);
    context.expect(
        controller == nullptr &&
            create_error.code ==
                LiveCaptureCreateErrorCode::invalid_output,
        "controller rejects output from a different venue");
}

void test_invalid_reconnect_policy_is_rejected(
    Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{
        "BTCUSDT"};
    SubscriptionError subscription_error;
    std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            subscription_error);
    boost::asio::io_context io_context;
    LiveCaptureCreateError create_error;
    LiveReconnectOptions reconnect_options;
    reconnect_options.initial_backoff =
        std::chrono::milliseconds{0};
    const std::shared_ptr<LiveCaptureController> controller =
        LiveCaptureController::create(
            io_context,
            std::move(subscription),
            nullptr,
            {},
            {},
            create_error,
            reconnect_options);
    context.expect(
        controller == nullptr &&
            create_error.code ==
                LiveCaptureCreateErrorCode::
                    invalid_reconnect_policy,
        "controller rejects a zero reconnect delay");
}

void test_stop_terminal_arbitration(Context& context) {
    const auto run_case = [&](
                              const WebSocketSessionErrorCode session_code,
                              const bool expect_success,
                              const std::uint64_t expected_recoverable_failures,
                              const std::string_view label) {
        constexpr std::array<std::string_view, 1U> symbols{
            "BTCUSDT"};
        SubscriptionError subscription_error;
        std::unique_ptr<LiveSubscription> subscription =
            LiveSubscription::create(
                PayloadVenue::spot,
                symbols.data(),
                symbols.size(),
                subscription_error);
        if (!subscription) {
            context.expect(false, "arbitration subscription is created");
            return;
        }
        const VerifiedWebSocketEndpoint endpoint =
            production_websocket_endpoint(*subscription);
        ControllerTemporaryDirectory parent;
        OutputSetOpenError output_error;
        std::unique_ptr<CsvOutputSet> output =
            CsvOutputSet::open_live(
                (parent.path() / "capture").string(),
                PayloadVenue::spot,
                symbols.data(),
                symbols.size(),
                "2026-08-05",
                output_error);
        if (!output) {
            context.expect(false, "arbitration output is created");
            return;
        }

        boost::asio::io_context io_context;
        std::uint32_t terminal_calls{0U};
        LiveCaptureCallbacks callbacks;
        callbacks.on_terminal =
            [&](const LiveCaptureResult&) { ++terminal_calls; };
        LiveCaptureCreateError create_error;
        const std::shared_ptr<LiveCaptureController> controller =
            LiveCaptureController::create(
                io_context,
                std::move(subscription),
                std::move(output),
                endpoint,
                std::move(callbacks),
                create_error);
        if (!controller) {
            context.expect(false, "arbitration controller is created");
            return;
        }

        controller->stop();
        LiveCaptureControllerTestAccess::on_session_terminal(
            *controller,
            1U,
            WebSocketSessionResult{
                session_code,
                WebSocketSessionStage::read,
                {},
                7U});
        io_context.run();

        const LiveCaptureResult& result = controller->result();
        context.expect(
            controller->terminal() && terminal_calls == 1U &&
                result.stop_requested &&
                result.session.code == session_code &&
                result.session.last_connection_sequence == 7U &&
                result.metrics.connection_invalidations == 1U &&
                result.metrics.recoverable_session_failures ==
                    expected_recoverable_failures &&
                result.metrics.reconnects_scheduled == 0U &&
                result.success() == expect_success &&
                result.error ==
                    (expect_success
                         ? LiveCaptureErrorCode::none
                         : LiveCaptureErrorCode::session_failure),
            label);
    };

    run_case(
        WebSocketSessionErrorCode::remote_close,
        true,
        1U,
        "queued recoverable terminal result loses to stop intent");
    run_case(
        WebSocketSessionErrorCode::tls_verification_failure,
        false,
        0U,
        "stop intent does not mask a non-recoverable session failure");
}

}  // namespace

void run_live_capture_controller_tests(Context& context) {
    test_output_venue_must_match_subscription(context);
    test_invalid_reconnect_policy_is_rejected(context);
    test_stop_terminal_arbitration(context);
}

}  // namespace hft::test
