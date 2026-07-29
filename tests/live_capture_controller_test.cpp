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

}  // namespace

void run_live_capture_controller_tests(Context& context) {
    test_output_venue_must_match_subscription(context);
    test_invalid_reconnect_policy_is_rejected(context);
}

}  // namespace hft::test
