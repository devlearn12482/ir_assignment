#pragma once

#include "hft/csv_output_set.h"
#include "hft/csv_writer.h"
#include "hft/live_event_pipeline.h"
#include "hft/live_subscription.h"
#include "hft/verified_websocket_session.h"

#include <boost/asio/io_context.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <string_view>

namespace hft {

enum class LiveCaptureCreateErrorCode : std::uint8_t {
    none,
    invalid_subscription,
    invalid_output,
    pipeline_initialization_failure,
    writer_initialization_failure,
    session_initialization_failure,
    allocation_failure,
};

struct LiveCaptureCreateError {
    LiveCaptureCreateErrorCode code{
        LiveCaptureCreateErrorCode::none};
    LivePipelineCreateError pipeline_error{
        LivePipelineCreateError::none};
    CsvWriterCreateError writer_error{
        CsvWriterCreateError::none};
    WebSocketSessionResult session_error{};

    [[nodiscard]] explicit operator bool() const noexcept {
        return code != LiveCaptureCreateErrorCode::none;
    }
};

enum class LiveCaptureErrorCode : std::uint8_t {
    none,
    session_failure,
    pipeline_failure,
    writer_acquire_failure,
    writer_publish_failure,
    writer_failure,
    backpressure_timeout,
    resume_notification_failure,
};

struct LiveCaptureMetrics {
    std::uint64_t text_messages{0};
    std::uint64_t batches_published{0};
    std::uint64_t pre_audit_rejections{0};
    std::uint64_t producer_pauses{0};
    std::uint64_t producer_resumes{0};
    std::uint64_t ping_frames{0};
    std::uint64_t pong_frames{0};
    std::uint64_t close_frames{0};
};

struct LiveCaptureResult {
    LiveCaptureErrorCode error{LiveCaptureErrorCode::none};
    WebSocketSessionResult session{};
    LivePipelineResult pipeline{};
    CsvWriterAcquireError writer_acquire_error{
        CsvWriterAcquireError::none};
    CsvWriterPublishError writer_publish_error{
        CsvWriterPublishError::none};
    CsvWriterResult writer{};
    LiveCaptureMetrics metrics{};
    bool stop_requested{false};

    [[nodiscard]] bool success() const noexcept {
        return error == LiveCaptureErrorCode::none &&
               stop_requested && writer.success();
    }
};

struct LiveCaptureCallbacks {
    // Runs on the I/O thread after the producer is quiescent and the writer
    // has completed its checked drain and join.
    std::function<void(const LiveCaptureResult&)> on_terminal{};
};

class LiveCaptureController final
    : public std::enable_shared_from_this<LiveCaptureController> {
public:
    // Takes ownership of the validated subscription and matching live output
    // set. Construction starts the writer thread but does not initiate any
    // network operation until start(). A started controller retains itself
    // until terminal writer drain completes on the I/O thread.
    [[nodiscard]] static std::shared_ptr<LiveCaptureController>
    create(
        boost::asio::io_context& io_context,
        std::unique_ptr<LiveSubscription> subscription,
        std::unique_ptr<CsvOutputSet> output,
        VerifiedWebSocketEndpoint endpoint,
        LiveCaptureCallbacks callbacks,
        LiveCaptureCreateError& error) noexcept;

    LiveCaptureController(const LiveCaptureController&) = delete;
    LiveCaptureController& operator=(
        const LiveCaptureController&) = delete;
    LiveCaptureController(LiveCaptureController&&) = delete;
    LiveCaptureController& operator=(
        LiveCaptureController&&) = delete;

    ~LiveCaptureController() noexcept;

    void start();
    void stop();

    [[nodiscard]] bool terminal() const noexcept;
    // Read only after terminal() is true or after the I/O context has
    // quiesced. The terminal callback receives the same immutable result.
    [[nodiscard]] const LiveCaptureResult& result() const noexcept;

private:
    LiveCaptureController(
        boost::asio::io_context& io_context,
        std::unique_ptr<LiveSubscription> subscription,
        LiveCaptureCallbacks callbacks);

    [[nodiscard]] bool output_matches_subscription(
        const CsvOutputSet& output) const noexcept;
    void on_open();
    void on_text_message(
        const WebSocketTextMessage& message);
    void on_control(
        const WebSocketControlFrame& frame) noexcept;
    void on_session_terminal(
        WebSocketSessionResult session_result) noexcept;
    void pause_for_backpressure();
    void post_resume_from_writer() noexcept;
    void on_writer_resume() noexcept;
    void on_backpressure_timeout(
        const boost::system::error_code& error) noexcept;
    void fail(LiveCaptureErrorCode error) noexcept;
    static void writer_resume_thunk(void* context) noexcept;

    struct Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const LiveCaptureCreateErrorCode code) noexcept {
    switch (code) {
        case LiveCaptureCreateErrorCode::none:
            return "none";
        case LiveCaptureCreateErrorCode::invalid_subscription:
            return "invalid_subscription";
        case LiveCaptureCreateErrorCode::invalid_output:
            return "invalid_output";
        case LiveCaptureCreateErrorCode::
                pipeline_initialization_failure:
            return "pipeline_initialization_failure";
        case LiveCaptureCreateErrorCode::
                writer_initialization_failure:
            return "writer_initialization_failure";
        case LiveCaptureCreateErrorCode::
                session_initialization_failure:
            return "session_initialization_failure";
        case LiveCaptureCreateErrorCode::allocation_failure:
            return "allocation_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const LiveCaptureErrorCode code) noexcept {
    switch (code) {
        case LiveCaptureErrorCode::none:
            return "none";
        case LiveCaptureErrorCode::session_failure:
            return "session_failure";
        case LiveCaptureErrorCode::pipeline_failure:
            return "pipeline_failure";
        case LiveCaptureErrorCode::writer_acquire_failure:
            return "writer_acquire_failure";
        case LiveCaptureErrorCode::writer_publish_failure:
            return "writer_publish_failure";
        case LiveCaptureErrorCode::writer_failure:
            return "writer_failure";
        case LiveCaptureErrorCode::backpressure_timeout:
            return "backpressure_timeout";
        case LiveCaptureErrorCode::resume_notification_failure:
            return "resume_notification_failure";
    }
    return "unknown";
}

}  // namespace hft
