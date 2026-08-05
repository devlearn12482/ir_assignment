#include "hft/live_capture_controller.h"

#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>
#include <new>
#include <random>
#include <utility>

namespace hft {
namespace {

constexpr auto kBackpressureTimeout = std::chrono::seconds{5};
constexpr std::uint64_t kMessagePolicyBreakerLimit = 3U;

void record_event_status(
    LiveCaptureMetrics& metrics,
    const EventProcessStatus status) noexcept {
    switch (status) {
        case EventProcessStatus::applied_refresh:
            ++metrics.applied_refreshes;
            break;
        case EventProcessStatus::applied_diff:
            ++metrics.applied_diffs;
            break;
        case EventProcessStatus::stale_refresh:
            ++metrics.stale_refreshes;
            break;
        case EventProcessStatus::stale_diff:
            ++metrics.stale_diffs;
            break;
        case EventProcessStatus::ignored_while_invalid:
            ++metrics.ignored_while_invalid;
            break;
        case EventProcessStatus::sequence_gap:
            ++metrics.sequence_gaps;
            break;
        case EventProcessStatus::crossed_book:
            ++metrics.crossed_books;
            break;
        case EventProcessStatus::trade_audited:
            ++metrics.trades_audited;
            break;
        case EventProcessStatus::schema_rejected:
            ++metrics.schema_rejections;
            break;
        case EventProcessStatus::not_processed:
        case EventProcessStatus::pre_audit_rejected:
            break;
    }
}

void record_envelope_rejection(
    LiveCaptureMetrics& metrics,
    const LiveEnvelopeErrorCode error) noexcept {
    if (error == LiveEnvelopeErrorCode::unknown_stream) {
        ++metrics.unknown_stream_rejections;
        return;
    }
    switch (error) {
        case LiveEnvelopeErrorCode::missing_data:
        case LiveEnvelopeErrorCode::duplicate_data:
        case LiveEnvelopeErrorCode::data_not_object:
        case LiveEnvelopeErrorCode::payload_minify_failed:
            ++metrics.invalid_data_rejections;
            return;
        case LiveEnvelopeErrorCode::none:
        case LiveEnvelopeErrorCode::unknown_stream:
            return;
        case LiveEnvelopeErrorCode::invalid_input_buffer:
        case LiveEnvelopeErrorCode::message_too_large:
        case LiveEnvelopeErrorCode::json_nesting_too_deep:
        case LiveEnvelopeErrorCode::malformed_json:
        case LiveEnvelopeErrorCode::root_not_object:
        case LiveEnvelopeErrorCode::missing_stream:
        case LiveEnvelopeErrorCode::duplicate_stream:
        case LiveEnvelopeErrorCode::stream_wrong_type:
        case LiveEnvelopeErrorCode::stream_name_too_long:
            ++metrics.malformed_envelope_rejections;
            return;
    }
}

[[nodiscard]] bool valid_reconnect_options(
    const LiveReconnectOptions& options) noexcept {
    return options.initial_backoff.count() > 0 &&
           options.maximum_backoff >= options.initial_backoff &&
           options.stable_connection_reset.count() > 0;
}

[[nodiscard]] bool recoverable_session_error(
    const WebSocketSessionErrorCode code) noexcept {
    switch (code) {
        case WebSocketSessionErrorCode::resolve_failure:
        case WebSocketSessionErrorCode::connect_failure:
        case WebSocketSessionErrorCode::
                websocket_handshake_failure:
        case WebSocketSessionErrorCode::tls_handshake_failure:
        case WebSocketSessionErrorCode::read_failure:
        case WebSocketSessionErrorCode::incomplete_message:
        case WebSocketSessionErrorCode::binary_message:
        case WebSocketSessionErrorCode::message_too_big:
        case WebSocketSessionErrorCode::remote_close:
        case WebSocketSessionErrorCode::timeout:
            return true;
        case WebSocketSessionErrorCode::none:
        case WebSocketSessionErrorCode::invalid_configuration:
        case WebSocketSessionErrorCode::allocation_failure:
        case WebSocketSessionErrorCode::trust_store_failure:
        case WebSocketSessionErrorCode::tls_policy_failure:
        case WebSocketSessionErrorCode::sni_failure:
        case WebSocketSessionErrorCode::
                tls_verification_failure:
        case WebSocketSessionErrorCode::sequence_overflow:
        case WebSocketSessionErrorCode::timestamp_failure:
        case WebSocketSessionErrorCode::close_failure:
        case WebSocketSessionErrorCode::callback_failure:
        case WebSocketSessionErrorCode::cancelled:
            return false;
    }
    return false;
}

}  // namespace

struct LiveCaptureController::Impl {
    struct WriterResumeBridge {
        std::weak_ptr<LiveCaptureController> controller{};
    };

    boost::asio::io_context& io_context;
    boost::asio::steady_timer backpressure_timer;
    boost::asio::steady_timer reconnect_timer;
    boost::asio::steady_timer stability_timer;
    std::unique_ptr<LiveSubscription> subscription;
    std::unique_ptr<LiveEventPipeline> pipeline;
    std::unique_ptr<CsvWriter> writer;
    std::shared_ptr<VerifiedWebSocketSession> session;
    std::shared_ptr<WriterResumeBridge> resume_bridge;
    std::shared_ptr<LiveCaptureController> run_lifetime;
    VerifiedWebSocketEndpoint endpoint;
    LiveCaptureCallbacks callbacks;
    LiveReconnectOptions reconnect_options;
    std::mt19937_64 random{std::random_device{}()};
    LiveCaptureResult result{};
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> terminal{false};
    std::atomic<bool> resume_post_failed{false};
    std::uint64_t generation{1U};
    std::uint64_t next_connection_epoch{0U};
    std::uint64_t reconnect_attempt{0U};
    std::uint64_t consecutive_message_policy_failures{0U};
    bool started{false};
    bool session_open{false};
    bool epoch_exhausted{false};
    bool reads_paused{false};
    bool failure_latched{false};
    bool terminal_started{false};

    Impl(
        boost::asio::io_context& configured_io_context,
        std::unique_ptr<LiveSubscription> configured_subscription,
        VerifiedWebSocketEndpoint configured_endpoint,
        LiveCaptureCallbacks configured_callbacks,
        LiveReconnectOptions configured_reconnect_options)
        : io_context{configured_io_context},
          backpressure_timer{configured_io_context},
          reconnect_timer{configured_io_context},
          stability_timer{configured_io_context},
          subscription{std::move(configured_subscription)},
          endpoint{std::move(configured_endpoint)},
          callbacks{std::move(configured_callbacks)},
          reconnect_options{
              std::move(configured_reconnect_options)} {}

    [[nodiscard]] std::chrono::milliseconds
    select_reconnect_delay() {
        const std::uint64_t initial = static_cast<std::uint64_t>(
            reconnect_options.initial_backoff.count());
        const std::uint64_t maximum = static_cast<std::uint64_t>(
            reconnect_options.maximum_backoff.count());
        std::uint64_t cap = initial;
        for (std::uint64_t index = 0U;
             index < reconnect_attempt && cap < maximum;
             ++index) {
            if (cap > maximum / 2U) {
                cap = maximum;
            } else {
                cap *= 2U;
            }
        }
        cap = std::min(cap, maximum);
        const std::uint64_t lower = cap / 2U;
        std::uint64_t selected = lower;
        if (reconnect_options.select_inclusive != nullptr) {
            selected =
                reconnect_options.select_inclusive(
                    reconnect_options.jitter_context,
                    lower,
                    cap);
            selected = std::max(lower, std::min(selected, cap));
        } else {
            std::uniform_int_distribution<std::uint64_t>
                distribution{lower, cap};
            selected = distribution(random);
        }
        if (reconnect_attempt !=
            std::numeric_limits<std::uint64_t>::max()) {
            ++reconnect_attempt;
        }
        return std::chrono::milliseconds{selected};
    }
};

std::shared_ptr<LiveCaptureController>
LiveCaptureController::create(
    boost::asio::io_context& io_context,
    std::unique_ptr<LiveSubscription> subscription,
    std::unique_ptr<CsvOutputSet> output,
    VerifiedWebSocketEndpoint endpoint,
    LiveCaptureCallbacks callbacks,
    LiveCaptureCreateError& error,
    LiveReconnectOptions reconnect_options) noexcept {
    error = {};
    if (!subscription || subscription->symbol_count() == 0U) {
        error.code =
            LiveCaptureCreateErrorCode::invalid_subscription;
        return nullptr;
    }
    if (!valid_reconnect_options(reconnect_options)) {
        error.code =
            LiveCaptureCreateErrorCode::invalid_reconnect_policy;
        return nullptr;
    }
    try {
        std::shared_ptr<LiveCaptureController> controller{
            new LiveCaptureController{
                io_context,
                std::move(subscription),
                std::move(endpoint),
                std::move(callbacks),
                std::move(reconnect_options)}};
        if (!output ||
            !controller->output_matches_subscription(*output)) {
            error.code = LiveCaptureCreateErrorCode::invalid_output;
            return nullptr;
        }

        controller->impl_->pipeline =
            LiveEventPipeline::create(
                *controller->impl_->subscription,
                error.pipeline_error);
        if (!controller->impl_->pipeline) {
            error.code = LiveCaptureCreateErrorCode::
                pipeline_initialization_failure;
            return nullptr;
        }

        controller->impl_->resume_bridge =
            std::make_shared<Impl::WriterResumeBridge>();
        controller->impl_->resume_bridge->controller =
            controller;
        const CsvWriterNotifications notifications{
            controller->impl_->resume_bridge.get(),
            &LiveCaptureController::writer_resume_thunk,
            &LiveCaptureController::writer_resume_thunk,
            controller->impl_->resume_bridge};
        controller->impl_->writer = CsvWriter::start(
            std::move(output),
            error.writer_error,
            notifications);
        if (!controller->impl_->writer) {
            error.code = LiveCaptureCreateErrorCode::
                writer_initialization_failure;
            return nullptr;
        }

        if (!controller->create_session(
                controller->impl_->generation,
                controller->impl_->next_connection_epoch,
                error.session_error)) {
            static_cast<void>(
                controller->impl_->writer->join());
            error.code = LiveCaptureCreateErrorCode::
                session_initialization_failure;
            return nullptr;
        }
        return controller;
    } catch (const std::bad_alloc&) {
        error.code = LiveCaptureCreateErrorCode::allocation_failure;
        return nullptr;
    } catch (...) {
        error.code = LiveCaptureCreateErrorCode::allocation_failure;
        return nullptr;
    }
}

LiveCaptureController::LiveCaptureController(
    boost::asio::io_context& io_context,
    std::unique_ptr<LiveSubscription> subscription,
    VerifiedWebSocketEndpoint endpoint,
    LiveCaptureCallbacks callbacks,
    LiveReconnectOptions reconnect_options)
    : impl_{std::make_unique<Impl>(
          io_context,
          std::move(subscription),
          std::move(endpoint),
          std::move(callbacks),
          std::move(reconnect_options))} {}

LiveCaptureController::~LiveCaptureController() noexcept {
    if (!impl_) {
        return;
    }
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
    impl_->reconnect_timer.cancel(ignored);
    impl_->stability_timer.cancel(ignored);
    if (impl_->session && !impl_->session->terminal()) {
        try {
            impl_->session->stop();
        } catch (...) {
        }
    }
    if (impl_->writer) {
        static_cast<void>(impl_->writer->join());
    }
}

bool LiveCaptureController::output_matches_subscription(
    const CsvOutputSet& output) const noexcept {
    if (!impl_ || output.mode() != CsvOutputMode::live_capture ||
        output.venue() != impl_->subscription->venue() ||
        output.closed() || output.failed() ||
        output.target_count() !=
            impl_->subscription->symbol_count()) {
        return false;
    }
    for (std::size_t index = 0U;
         index < output.target_count();
         ++index) {
        if (output.symbol(index) !=
            impl_->subscription->symbol(index)) {
            return false;
        }
    }
    return true;
}

bool LiveCaptureController::create_session(
    const std::uint64_t generation,
    const std::uint64_t connection_epoch,
    WebSocketSessionResult& error) noexcept {
    const std::weak_ptr<LiveCaptureController> weak{
        weak_from_this()};
    WebSocketSessionCallbacks session_callbacks;
    session_callbacks.on_open = [weak, generation]() {
        if (const auto owner = weak.lock()) {
            owner->on_open(generation);
        }
    };
    session_callbacks.on_text_message =
        [weak, generation](
            const WebSocketTextMessage& message) {
            if (const auto owner = weak.lock()) {
                owner->on_text_message(generation, message);
            }
        };
    session_callbacks.on_control =
        [weak, generation](
            const WebSocketControlFrame& frame) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_control(generation, frame);
            }
        };
    session_callbacks.on_terminal =
        [weak, generation](
            WebSocketSessionResult result) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_session_terminal(
                    generation, result);
            }
        };
    impl_->session = VerifiedWebSocketSession::create(
        impl_->io_context,
        impl_->endpoint,
        std::move(session_callbacks),
        connection_epoch,
        error);
    return impl_->session != nullptr;
}

void LiveCaptureController::start() {
    if (!impl_ || impl_->started ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->started = true;
    impl_->run_lifetime = shared_from_this();
    try {
        start_current_session();
    } catch (...) {
        impl_->run_lifetime.reset();
        impl_->started = false;
        throw;
    }
}

void LiveCaptureController::start_current_session() {
    if (!impl_->session) {
        fail(LiveCaptureErrorCode::reconnect_scheduling_failure);
        return;
    }
    ++impl_->result.metrics.connection_attempts;
    impl_->session->start();
}

void LiveCaptureController::stop() {
    if (!impl_ ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->stop_requested.store(true, std::memory_order_release);
    const std::weak_ptr<LiveCaptureController> weak{
        weak_from_this()};
    boost::asio::post(
        impl_->io_context,
        [weak]() noexcept {
            if (const auto owner = weak.lock()) {
                owner->request_stop_on_io();
            }
        });
}

void LiveCaptureController::request_stop_on_io() noexcept {
    if (impl_->terminal.load(std::memory_order_acquire) ||
        impl_->terminal_started) {
        return;
    }
    boost::system::error_code ignored;
    impl_->reconnect_timer.cancel(ignored);
    impl_->stability_timer.cancel(ignored);
    if (impl_->session && !impl_->session->terminal()) {
        try {
            impl_->session->stop();
            return;
        } catch (...) {
            handle_session_stop_exception();
            return;
        }
    }
    finalize_run();
}

void LiveCaptureController::handle_session_stop_exception() noexcept {
    if (!impl_->failure_latched) {
        impl_->failure_latched = true;
        impl_->result.error = LiveCaptureErrorCode::session_failure;
    }
    finalize_run();
}

bool LiveCaptureController::terminal() const noexcept {
    return impl_ &&
           impl_->terminal.load(std::memory_order_acquire);
}

const LiveCaptureResult& LiveCaptureController::result()
    const noexcept {
    return impl_->result;
}

void LiveCaptureController::on_open(
    const std::uint64_t generation) {
    if (generation != impl_->generation) {
        ++impl_->result.metrics.stale_session_callbacks;
        return;
    }
    impl_->session_open = true;
    ++impl_->result.metrics.successful_connections;
    if (impl_->next_connection_epoch ==
        std::numeric_limits<std::uint64_t>::max()) {
        impl_->epoch_exhausted = true;
    } else {
        ++impl_->next_connection_epoch;
    }

    impl_->stability_timer.expires_after(
        impl_->reconnect_options.stable_connection_reset);
    const std::weak_ptr<LiveCaptureController> weak{
        weak_from_this()};
    impl_->stability_timer.async_wait(
        [weak, generation](
            const boost::system::error_code& error) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_stable_connection(
                    generation, error);
            }
        });

    if (impl_->stop_requested.load(std::memory_order_acquire)) {
        request_stop_on_io();
        return;
    }
    if (impl_->writer->should_pause()) {
        pause_for_backpressure();
    }
}

void LiveCaptureController::on_stable_connection(
    const std::uint64_t generation,
    const boost::system::error_code& error) noexcept {
    if (error == boost::asio::error::operation_aborted ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (generation != impl_->generation ||
        !impl_->session_open) {
        ++impl_->result.metrics.stale_session_callbacks;
        return;
    }
    impl_->reconnect_attempt = 0U;
    ++impl_->result.metrics.backoff_resets;
}

void LiveCaptureController::on_text_message(
    const std::uint64_t generation,
    const WebSocketTextMessage& message) {
    if (generation != impl_->generation) {
        ++impl_->result.metrics.stale_session_callbacks;
        return;
    }
    if (impl_->terminal.load(std::memory_order_acquire) ||
        impl_->failure_latched) {
        return;
    }
    ++impl_->result.metrics.text_messages;
    ++impl_->result.metrics.complete_messages;

    CsvWriterAcquireError acquire_error;
    EventRowBatch* const batch =
        impl_->writer->try_acquire(acquire_error);
    if (batch == nullptr) {
        impl_->result.writer_acquire_error = acquire_error;
        fail(
            impl_->writer->failed()
                ? LiveCaptureErrorCode::writer_failure
                : LiveCaptureErrorCode::writer_acquire_failure);
        return;
    }

    const LivePipelineResult pipeline_result =
        impl_->pipeline->process(message, *batch);
    impl_->result.pipeline = pipeline_result;
    if (pipeline_result.fatal()) {
        impl_->writer->cancel();
        fail(LiveCaptureErrorCode::pipeline_failure);
        return;
    }
    if (!pipeline_result.has_batch()) {
        impl_->writer->cancel();
        if (pipeline_result.disposition ==
            LivePipelineDisposition::pre_audit_rejected) {
            ++impl_->result.metrics.pre_audit_rejections;
            record_envelope_rejection(
                impl_->result.metrics,
                pipeline_result.envelope_error);
            return;
        }
        fail(LiveCaptureErrorCode::pipeline_failure);
        return;
    }

    ++impl_->result.metrics.audit_eligible_events;
    ++impl_->result.metrics.processed_events;
    record_event_status(
        impl_->result.metrics,
        pipeline_result.event.status);

    const CsvWriterPublishError publish_error =
        impl_->writer->publish(pipeline_result.target_index);
    if (publish_error != CsvWriterPublishError::none) {
        impl_->result.writer_publish_error = publish_error;
        fail(
            impl_->writer->failed()
                ? LiveCaptureErrorCode::writer_failure
                : LiveCaptureErrorCode::writer_publish_failure);
        return;
    }
    ++impl_->result.metrics.batches_published;
    impl_->consecutive_message_policy_failures = 0U;
    if (impl_->writer->should_pause()) {
        pause_for_backpressure();
    }
}

void LiveCaptureController::on_control(
    const std::uint64_t generation,
    const WebSocketControlFrame& frame) noexcept {
    if (generation != impl_->generation) {
        ++impl_->result.metrics.stale_session_callbacks;
        return;
    }
    switch (frame.kind) {
        case WebSocketControlKind::ping:
            ++impl_->result.metrics.ping_frames;
            break;
        case WebSocketControlKind::pong:
            ++impl_->result.metrics.pong_frames;
            break;
        case WebSocketControlKind::close:
            ++impl_->result.metrics.close_frames;
            break;
    }
}

void LiveCaptureController::on_session_terminal(
    const std::uint64_t generation,
    const WebSocketSessionResult session_result) noexcept {
    if (generation != impl_->generation) {
        ++impl_->result.metrics.stale_session_callbacks;
        return;
    }
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
    impl_->stability_timer.cancel(ignored);
    impl_->reads_paused = false;
    impl_->session_open = false;
    impl_->pipeline->invalidate_all();
    impl_->result.metrics.connection_invalidations +=
        static_cast<std::uint64_t>(
            impl_->subscription->symbol_count());
    impl_->result.session = session_result;
    impl_->session.reset();

    if (session_result.code ==
        WebSocketSessionErrorCode::binary_message) {
        ++impl_->result.metrics.complete_messages;
        ++impl_->result.metrics.pre_audit_rejections;
        ++impl_->result.metrics.binary_messages;
        ++impl_->consecutive_message_policy_failures;
    } else if (session_result.code ==
               WebSocketSessionErrorCode::message_too_big) {
        ++impl_->result.metrics.oversized_messages;
        ++impl_->consecutive_message_policy_failures;
    }

    if (impl_->consecutive_message_policy_failures >=
        kMessagePolicyBreakerLimit) {
        ++impl_->result.metrics.message_policy_breaker_trips;
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::message_policy_breaker;
    }

    impl_->result.stop_requested =
        impl_->stop_requested.load(std::memory_order_acquire);
    if (impl_->resume_post_failed.load(
            std::memory_order_acquire)) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::resume_notification_failure;
    }
    if (impl_->result.stop_requested) {
        const bool recoverable_stop_result =
            recoverable_session_error(session_result.code);
        if (recoverable_stop_result &&
            !impl_->failure_latched) {
            ++impl_->result.metrics.recoverable_session_failures;
        }
        const bool expected_stop_result =
            session_result.success() ||
            session_result.code ==
                WebSocketSessionErrorCode::cancelled ||
            recoverable_stop_result;
        if (!expected_stop_result &&
            !impl_->failure_latched) {
            impl_->failure_latched = true;
            impl_->result.error =
                LiveCaptureErrorCode::session_failure;
        }
        finalize_run();
        return;
    }
    if (impl_->failure_latched) {
        finalize_run();
        return;
    }
    if (!recoverable_session_error(session_result.code)) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::session_failure;
        finalize_run();
        return;
    }
    if (impl_->epoch_exhausted) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::connection_epoch_overflow;
        finalize_run();
        return;
    }
    ++impl_->result.metrics.recoverable_session_failures;
    schedule_reconnect();
}

void LiveCaptureController::schedule_reconnect() noexcept {
    try {
        const std::chrono::milliseconds delay =
            impl_->select_reconnect_delay();
        ++impl_->result.metrics.reconnects_scheduled;
        impl_->result.metrics.last_reconnect_delay_ms =
            static_cast<std::uint64_t>(delay.count());
        impl_->reconnect_timer.expires_after(delay);
        const std::weak_ptr<LiveCaptureController> weak{
            weak_from_this()};
        impl_->reconnect_timer.async_wait(
            [weak](
                const boost::system::error_code& error) noexcept {
                if (const auto owner = weak.lock()) {
                    owner->on_reconnect_timer(error);
                }
            });
    } catch (...) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::reconnect_scheduling_failure;
        finalize_run();
    }
}

void LiveCaptureController::on_reconnect_timer(
    const boost::system::error_code& error) noexcept {
    if (error == boost::asio::error::operation_aborted ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (error) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::reconnect_scheduling_failure;
        finalize_run();
        return;
    }
    if (impl_->stop_requested.load(std::memory_order_acquire)) {
        finalize_run();
        return;
    }
    if (impl_->generation ==
        std::numeric_limits<std::uint64_t>::max()) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::reconnect_scheduling_failure;
        finalize_run();
        return;
    }
    ++impl_->generation;
    WebSocketSessionResult create_error;
    if (!create_session(
            impl_->generation,
            impl_->next_connection_epoch,
            create_error)) {
        impl_->result.session = create_error;
        impl_->failure_latched = true;
        impl_->result.error = LiveCaptureErrorCode::session_failure;
        finalize_run();
        return;
    }
    try {
        start_current_session();
    } catch (...) {
        impl_->failure_latched = true;
        impl_->result.error =
            LiveCaptureErrorCode::reconnect_scheduling_failure;
        finalize_run();
    }
}

void LiveCaptureController::finalize_run() noexcept {
    if (impl_->terminal_started) {
        return;
    }
    impl_->terminal_started = true;
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
    impl_->reconnect_timer.cancel(ignored);
    impl_->stability_timer.cancel(ignored);
    impl_->result.stop_requested =
        impl_->stop_requested.load(std::memory_order_acquire);
    impl_->result.writer = impl_->writer->join();
    if (!impl_->result.writer.success() &&
        impl_->result.error == LiveCaptureErrorCode::none) {
        impl_->result.error = LiveCaptureErrorCode::writer_failure;
    }
    impl_->terminal.store(true, std::memory_order_release);
    auto terminal_callback =
        std::move(impl_->callbacks.on_terminal);
    impl_->callbacks = {};
    if (terminal_callback) {
        try {
            terminal_callback(impl_->result);
        } catch (...) {
        }
    }
    impl_->run_lifetime.reset();
}

void LiveCaptureController::pause_for_backpressure() {
    if (impl_->reads_paused || impl_->failure_latched ||
        !impl_->session) {
        return;
    }
    impl_->reads_paused = true;
    ++impl_->result.metrics.producer_pauses;
    impl_->session->pause_reads();
    impl_->backpressure_timer.expires_after(
        kBackpressureTimeout);
    const std::weak_ptr<LiveCaptureController> weak{
        weak_from_this()};
    impl_->backpressure_timer.async_wait(
        [weak](const boost::system::error_code& error) noexcept {
            if (const auto owner = weak.lock()) {
                owner->on_backpressure_timeout(error);
            }
        });
    impl_->writer->arm_resume_notification();
}

void LiveCaptureController::writer_resume_thunk(
    void* const context) noexcept {
    if (context != nullptr) {
        auto& bridge =
            *static_cast<Impl::WriterResumeBridge*>(context);
        if (const auto owner = bridge.controller.lock()) {
            owner->post_resume_from_writer();
        }
    }
}

void LiveCaptureController::post_resume_from_writer() noexcept {
    try {
        const std::weak_ptr<LiveCaptureController> weak{
            weak_from_this()};
        boost::asio::post(
            impl_->io_context,
            [weak]() noexcept {
                if (const auto owner = weak.lock()) {
                    owner->on_writer_resume();
                }
            });
    } catch (...) {
        impl_->resume_post_failed.store(
            true, std::memory_order_release);
        try {
            if (impl_->session) {
                impl_->session->stop();
            }
        } catch (...) {
            impl_->io_context.stop();
        }
    }
}

void LiveCaptureController::on_writer_resume() noexcept {
    if (impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (impl_->resume_post_failed.load(std::memory_order_acquire)) {
        fail(LiveCaptureErrorCode::resume_notification_failure);
        return;
    }
    if (impl_->writer->failed()) {
        fail(LiveCaptureErrorCode::writer_failure);
        return;
    }
    if (!impl_->reads_paused) {
        return;
    }
    if (!impl_->writer->below_resume_watermark()) {
        impl_->writer->arm_resume_notification();
        return;
    }
    impl_->reads_paused = false;
    ++impl_->result.metrics.producer_resumes;
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
    try {
        if (impl_->session) {
            impl_->session->resume_reads();
        }
    } catch (...) {
        fail(LiveCaptureErrorCode::resume_notification_failure);
    }
}

void LiveCaptureController::on_backpressure_timeout(
    const boost::system::error_code& error) noexcept {
    if (error == boost::asio::error::operation_aborted ||
        impl_->terminal.load(std::memory_order_acquire) ||
        !impl_->reads_paused) {
        return;
    }
    fail(
        impl_->writer->failed()
            ? LiveCaptureErrorCode::writer_failure
            : LiveCaptureErrorCode::backpressure_timeout);
}

void LiveCaptureController::fail(
    const LiveCaptureErrorCode error) noexcept {
    if (impl_->failure_latched ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->failure_latched = true;
    impl_->result.error = error;
    boost::system::error_code ignored;
    impl_->reconnect_timer.cancel(ignored);
    impl_->stability_timer.cancel(ignored);
    try {
        if (impl_->session && !impl_->session->terminal()) {
            impl_->session->pause_reads();
            impl_->session->stop();
            return;
        }
    } catch (...) {
        handle_session_stop_exception();
        return;
    }
    finalize_run();
}

}  // namespace hft
