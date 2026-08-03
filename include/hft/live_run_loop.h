#pragma once

#include "hft/live_capture_controller.h"

#include <boost/asio/io_context.hpp>

#include <cstdint>
#include <memory>
#include <optional>

namespace hft {

enum class LiveRunLoopCreateError : std::uint8_t {
    none,
    invalid_duration,
    initialization_failure,
};

enum class LiveRunLoopErrorCode : std::uint8_t {
    none,
    signal_wait_failure,
    duration_wait_failure,
    stop_request_failure,
};

struct LiveRunLoopResult {
    LiveCaptureResult capture{};
    LiveRunLoopErrorCode error{LiveRunLoopErrorCode::none};
    std::uint64_t signals_received{0};
    std::uint64_t repeated_signals{0};
    bool duration_expired{false};
    bool terminal{false};
};

class LiveRunLoop final
    : public std::enable_shared_from_this<LiveRunLoop> {
public:
    [[nodiscard]] static std::shared_ptr<LiveRunLoop> create(
        boost::asio::io_context& io_context,
        std::optional<std::uint64_t> duration_seconds,
        LiveRunLoopCreateError& error) noexcept;

    LiveRunLoop(const LiveRunLoop&) = delete;
    LiveRunLoop& operator=(const LiveRunLoop&) = delete;
    LiveRunLoop(LiveRunLoop&&) = delete;
    LiveRunLoop& operator=(LiveRunLoop&&) = delete;

    ~LiveRunLoop() noexcept;

    // Obtain callbacks before attaching the controller. The callbacks retain
    // no owning cycle and remain valid while the caller retains this loop.
    [[nodiscard]] LiveCaptureCallbacks capture_callbacks();
    [[nodiscard]] bool attach_controller(
        std::shared_ptr<LiveCaptureController> controller) noexcept;
    void start();

    [[nodiscard]] bool terminal() const noexcept;
    [[nodiscard]] const LiveRunLoopResult& result() const noexcept;

private:
    struct Impl;

    LiveRunLoop(
        boost::asio::io_context& io_context,
        std::optional<std::uint64_t> duration_seconds);

    void arm_signal_wait();
    void on_signal(
        const boost::system::error_code& error,
        int signal_number) noexcept;
    void on_duration(
        const boost::system::error_code& error) noexcept;
    void on_capture_terminal(
        const LiveCaptureResult& result) noexcept;
    void request_stop() noexcept;

    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const LiveRunLoopCreateError error) noexcept {
    switch (error) {
        case LiveRunLoopCreateError::none:
            return "none";
        case LiveRunLoopCreateError::invalid_duration:
            return "invalid_duration";
        case LiveRunLoopCreateError::initialization_failure:
            return "initialization_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const LiveRunLoopErrorCode error) noexcept {
    switch (error) {
        case LiveRunLoopErrorCode::none:
            return "none";
        case LiveRunLoopErrorCode::signal_wait_failure:
            return "signal_wait_failure";
        case LiveRunLoopErrorCode::duration_wait_failure:
            return "duration_wait_failure";
        case LiveRunLoopErrorCode::stop_request_failure:
            return "stop_request_failure";
    }
    return "unknown";
}

}  // namespace hft
