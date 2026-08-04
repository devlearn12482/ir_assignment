#pragma once

#include "hft/csv_output_set.h"
#include "hft/csv_writer.h"
#include "hft/live_event_pipeline.h"
#include "hft/live_subscription.h"
#include "hft/verified_websocket_session.h"

#include <boost/asio/io_context.hpp>

#include <chrono>
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
    invalid_reconnect_policy,
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
    message_policy_breaker,
    connection_epoch_overflow,
    reconnect_scheduling_failure,
};

struct LiveReconnectOptions {
    // Production uses equal-jitter exponential backoff. Tests may supply a
    // deterministic inclusive selector; jitter_lifetime keeps its context
    // alive for the full controller run.
    std::chrono::milliseconds initial_backoff{250};
    std::chrono::milliseconds maximum_backoff{30'000};
    std::chrono::milliseconds stable_connection_reset{30'000};
    void* jitter_context{nullptr};
    std::uint64_t (*select_inclusive)(
        void*,
        std::uint64_t,
        std::uint64_t) noexcept{nullptr};
    std::shared_ptr<void> jitter_lifetime{};
};

struct LiveCaptureMetrics {
    // complete_messages counts complete text callbacks plus complete binary
    // messages rejected by the session. Oversized/incomplete messages are
    // excluded because no complete logical message was available.
    std::uint64_t text_messages{0};
    std::uint64_t complete_messages{0};
    std::uint64_t batches_published{0};
    std::uint64_t audit_eligible_events{0};
    std::uint64_t processed_events{0};
    std::uint64_t pre_audit_rejections{0};
    std::uint64_t unknown_stream_rejections{0};
    std::uint64_t invalid_data_rejections{0};
    std::uint64_t malformed_envelope_rejections{0};
    std::uint64_t schema_rejections{0};
    std::uint64_t applied_refreshes{0};
    std::uint64_t applied_diffs{0};
    std::uint64_t stale_refreshes{0};
    std::uint64_t stale_diffs{0};
    std::uint64_t ignored_while_invalid{0};
    std::uint64_t sequence_gaps{0};
    std::uint64_t crossed_books{0};
    std::uint64_t trades_audited{0};
    std::uint64_t connection_invalidations{0};
    std::uint64_t connection_attempts{0};
    std::uint64_t successful_connections{0};
    std::uint64_t reconnects_scheduled{0};
    std::uint64_t recoverable_session_failures{0};
    std::uint64_t binary_messages{0};
    std::uint64_t oversized_messages{0};
    std::uint64_t message_policy_breaker_trips{0};
    std::uint64_t backoff_resets{0};
    std::uint64_t stale_session_callbacks{0};
    std::uint64_t last_reconnect_delay_ms{0};
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
        LiveCaptureCreateError& error,
        LiveReconnectOptions reconnect_options = {}) noexcept;

    LiveCaptureController(const LiveCaptureController&) = delete;
    LiveCaptureController& operator=(
        const LiveCaptureController&) = delete;
    LiveCaptureController(LiveCaptureController&&) = delete;
    LiveCaptureController& operator=(
        LiveCaptureController&&) = delete;

    ~LiveCaptureController() noexcept;

    void start();
    // Stop intent is published before its I/O handler is posted. A
    // recoverable session completion that was already queued therefore
    // converges on the same orderly shutdown result.
    void stop();

    [[nodiscard]] bool terminal() const noexcept;
    // Read only after terminal() is true or after the I/O context has
    // quiesced. The terminal callback receives the same immutable result.
    [[nodiscard]] const LiveCaptureResult& result() const noexcept;

private:
    friend struct LiveCaptureControllerTestAccess;

    LiveCaptureController(
        boost::asio::io_context& io_context,
        std::unique_ptr<LiveSubscription> subscription,
        VerifiedWebSocketEndpoint endpoint,
        LiveCaptureCallbacks callbacks,
        LiveReconnectOptions reconnect_options);

    [[nodiscard]] bool output_matches_subscription(
        const CsvOutputSet& output) const noexcept;
    [[nodiscard]] bool create_session(
        std::uint64_t generation,
        std::uint64_t connection_epoch,
        WebSocketSessionResult& error) noexcept;
    void start_current_session();
    void on_open(std::uint64_t generation);
    void on_text_message(
        std::uint64_t generation,
        const WebSocketTextMessage& message);
    void on_control(
        std::uint64_t generation,
        const WebSocketControlFrame& frame) noexcept;
    void on_session_terminal(
        std::uint64_t generation,
        WebSocketSessionResult session_result) noexcept;
    void schedule_reconnect() noexcept;
    void on_reconnect_timer(
        const boost::system::error_code& error) noexcept;
    void on_stable_connection(
        std::uint64_t generation,
        const boost::system::error_code& error) noexcept;
    void request_stop_on_io() noexcept;
    void finalize_run() noexcept;
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
        case LiveCaptureCreateErrorCode::invalid_reconnect_policy:
            return "invalid_reconnect_policy";
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
        case LiveCaptureErrorCode::message_policy_breaker:
            return "message_policy_breaker";
        case LiveCaptureErrorCode::connection_epoch_overflow:
            return "connection_epoch_overflow";
        case LiveCaptureErrorCode::reconnect_scheduling_failure:
            return "reconnect_scheduling_failure";
    }
    return "unknown";
}

}  // namespace hft
