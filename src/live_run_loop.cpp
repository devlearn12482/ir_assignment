#include "hft/live_run_loop.h"

#include <boost/asio/error.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <limits>
#include <new>
#include <utility>

namespace hft {
namespace {

[[nodiscard]] bool valid_duration(
    const std::optional<std::uint64_t> seconds) noexcept {
    if (!seconds.has_value() || *seconds == 0U) {
        return !seconds.has_value();
    }
    using TimerDuration = boost::asio::steady_timer::duration;
    const auto maximum_seconds =
        std::chrono::duration_cast<std::chrono::seconds>(
            TimerDuration::max())
            .count();
    return maximum_seconds > 0 &&
           *seconds <=
               static_cast<std::uint64_t>(maximum_seconds);
}

void latch_control_error(
    LiveRunLoopResult& result,
    const LiveRunLoopErrorCode error) noexcept {
    if (result.error == LiveRunLoopErrorCode::none) {
        result.error = error;
    }
}

}  // namespace

struct LiveRunLoop::Impl {
    boost::asio::io_context& io_context;
    boost::asio::signal_set signals;
    boost::asio::steady_timer duration_timer;
    std::optional<std::uint64_t> duration_seconds;
    std::shared_ptr<LiveCaptureController> controller{};
    LiveRunLoopResult result{};
    std::atomic<bool> terminal{false};
    bool started{false};
    bool stop_started{false};
    bool terminal_started{false};

    Impl(
        boost::asio::io_context& configured_io_context,
        std::optional<std::uint64_t> configured_duration)
        : io_context{configured_io_context},
          signals{configured_io_context, SIGINT, SIGTERM},
          duration_timer{configured_io_context},
          duration_seconds{configured_duration} {}
};

std::shared_ptr<LiveRunLoop> LiveRunLoop::create(
    boost::asio::io_context& io_context,
    const std::optional<std::uint64_t> duration_seconds,
    LiveRunLoopCreateError& error) noexcept {
    error = LiveRunLoopCreateError::none;
    if (!valid_duration(duration_seconds)) {
        error = LiveRunLoopCreateError::invalid_duration;
        return nullptr;
    }
    try {
        return std::shared_ptr<LiveRunLoop>{
            new LiveRunLoop{io_context, duration_seconds}};
    } catch (...) {
        error = LiveRunLoopCreateError::initialization_failure;
        return nullptr;
    }
}

LiveRunLoop::LiveRunLoop(
    boost::asio::io_context& io_context,
    std::optional<std::uint64_t> duration_seconds)
    : impl_{std::make_unique<Impl>(
          io_context, duration_seconds)} {}

LiveRunLoop::~LiveRunLoop() noexcept {
    if (!impl_) {
        return;
    }
    boost::system::error_code ignored;
    impl_->signals.cancel(ignored);
    impl_->duration_timer.cancel(ignored);
    if (impl_->controller && !impl_->controller->terminal()) {
        try {
            impl_->controller->stop();
        } catch (...) {
        }
    }
}

LiveCaptureCallbacks LiveRunLoop::capture_callbacks() {
    const std::weak_ptr<LiveRunLoop> weak{weak_from_this()};
    LiveCaptureCallbacks callbacks;
    callbacks.on_terminal =
        [weak](const LiveCaptureResult& result) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_capture_terminal(result);
            }
        };
    return callbacks;
}

bool LiveRunLoop::attach_controller(
    std::shared_ptr<LiveCaptureController> controller) noexcept {
    if (!controller || impl_->controller || impl_->started ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return false;
    }
    impl_->controller = std::move(controller);
    return true;
}

void LiveRunLoop::start() {
    if (impl_->started || !impl_->controller ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->started = true;
    arm_signal_wait();
    if (impl_->duration_seconds.has_value()) {
        impl_->duration_timer.expires_after(
            std::chrono::seconds{*impl_->duration_seconds});
        const std::weak_ptr<LiveRunLoop> weak{weak_from_this()};
        impl_->duration_timer.async_wait(
            [weak](
                const boost::system::error_code& error) noexcept {
                if (const auto owner = weak.lock()) {
                    owner->on_duration(error);
                }
            });
    }
    impl_->controller->start();
}

void LiveRunLoop::arm_signal_wait() {
    const std::weak_ptr<LiveRunLoop> weak{weak_from_this()};
    impl_->signals.async_wait(
        [weak](
            const boost::system::error_code& error,
            const int signal_number) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_signal(error, signal_number);
            }
        });
}

void LiveRunLoop::on_signal(
    const boost::system::error_code& error,
    const int signal_number) noexcept {
    static_cast<void>(signal_number);
    if (error == boost::asio::error::operation_aborted ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (error) {
        latch_control_error(
            impl_->result,
            LiveRunLoopErrorCode::signal_wait_failure);
        request_stop();
        return;
    }
    ++impl_->result.signals_received;
    if (impl_->stop_started) {
        ++impl_->result.repeated_signals;
    }
    try {
        arm_signal_wait();
    } catch (...) {
        latch_control_error(
            impl_->result,
            LiveRunLoopErrorCode::signal_wait_failure);
        request_stop();
        return;
    }
    request_stop();
}

void LiveRunLoop::on_duration(
    const boost::system::error_code& error) noexcept {
    if (error == boost::asio::error::operation_aborted ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (error) {
        latch_control_error(
            impl_->result,
            LiveRunLoopErrorCode::duration_wait_failure);
    } else {
        impl_->result.duration_expired = true;
    }
    request_stop();
}

void LiveRunLoop::request_stop() noexcept {
    impl_->stop_started = true;
    try {
        impl_->controller->stop();
    } catch (...) {
        latch_control_error(
            impl_->result,
            LiveRunLoopErrorCode::stop_request_failure);
        impl_->io_context.stop();
    }
}

void LiveRunLoop::on_capture_terminal(
    const LiveCaptureResult& result) noexcept {
    if (impl_->terminal_started) {
        return;
    }
    impl_->terminal_started = true;
    impl_->result.capture = result;
    impl_->result.terminal = true;
    boost::system::error_code ignored;
    impl_->duration_timer.cancel(ignored);
    impl_->signals.cancel(ignored);
    impl_->controller.reset();
    impl_->terminal.store(true, std::memory_order_release);
}

bool LiveRunLoop::terminal() const noexcept {
    return impl_ &&
           impl_->terminal.load(std::memory_order_acquire);
}

const LiveRunLoopResult& LiveRunLoop::result() const noexcept {
    return impl_->result;
}

}  // namespace hft
