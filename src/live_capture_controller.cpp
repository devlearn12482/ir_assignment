#include "hft/live_capture_controller.h"

#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <chrono>
#include <new>
#include <utility>

namespace hft {
namespace {

constexpr auto kBackpressureTimeout = std::chrono::seconds{5};

}  // namespace

struct LiveCaptureController::Impl {
    struct WriterResumeBridge {
        std::weak_ptr<LiveCaptureController> controller{};
    };

    boost::asio::io_context& io_context;
    boost::asio::steady_timer backpressure_timer;
    std::unique_ptr<LiveSubscription> subscription;
    std::unique_ptr<LiveEventPipeline> pipeline;
    std::unique_ptr<CsvWriter> writer;
    std::shared_ptr<VerifiedWebSocketSession> session;
    std::shared_ptr<WriterResumeBridge> resume_bridge;
    std::shared_ptr<LiveCaptureController> run_lifetime;
    LiveCaptureCallbacks callbacks;
    LiveCaptureResult result{};
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> terminal{false};
    std::atomic<bool> resume_post_failed{false};
    bool started{false};
    bool reads_paused{false};
    bool failure_latched{false};
    bool terminal_started{false};

    Impl(
        boost::asio::io_context& configured_io_context,
        std::unique_ptr<LiveSubscription> configured_subscription,
        LiveCaptureCallbacks configured_callbacks)
        : io_context{configured_io_context},
          backpressure_timer{configured_io_context},
          subscription{std::move(configured_subscription)},
          callbacks{std::move(configured_callbacks)} {}
};

std::shared_ptr<LiveCaptureController>
LiveCaptureController::create(
    boost::asio::io_context& io_context,
    std::unique_ptr<LiveSubscription> subscription,
    std::unique_ptr<CsvOutputSet> output,
    VerifiedWebSocketEndpoint endpoint,
    LiveCaptureCallbacks callbacks,
    LiveCaptureCreateError& error) noexcept {
    error = {};
    if (!subscription || subscription->symbol_count() == 0U) {
        error.code =
            LiveCaptureCreateErrorCode::invalid_subscription;
        return nullptr;
    }
    try {
        std::shared_ptr<LiveCaptureController> controller{
            new LiveCaptureController{
                io_context,
                std::move(subscription),
                std::move(callbacks)}};
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

        const std::weak_ptr<LiveCaptureController> weak{
            controller};
        WebSocketSessionCallbacks session_callbacks;
        session_callbacks.on_open = [weak]() {
            if (const auto owner = weak.lock()) {
                owner->on_open();
            }
        };
        session_callbacks.on_text_message =
            [weak](const WebSocketTextMessage& message) {
                if (const auto owner = weak.lock()) {
                    owner->on_text_message(message);
                }
            };
        session_callbacks.on_control =
            [weak](const WebSocketControlFrame& frame) noexcept {
                if (const auto owner = weak.lock()) {
                    owner->on_control(frame);
                }
            };
        session_callbacks.on_terminal =
            [weak](WebSocketSessionResult result) noexcept {
                if (const auto owner = weak.lock()) {
                    owner->on_session_terminal(result);
                }
            };
        controller->impl_->session =
            VerifiedWebSocketSession::create(
                io_context,
                std::move(endpoint),
                std::move(session_callbacks),
                error.session_error);
        if (!controller->impl_->session) {
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
    LiveCaptureCallbacks callbacks)
    : impl_{std::make_unique<Impl>(
          io_context,
          std::move(subscription),
          std::move(callbacks))} {}

LiveCaptureController::~LiveCaptureController() noexcept {
    if (!impl_) {
        return;
    }
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
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

void LiveCaptureController::start() {
    if (!impl_ || impl_->started ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->started = true;
    impl_->run_lifetime = shared_from_this();
    try {
        impl_->session->start();
    } catch (...) {
        impl_->run_lifetime.reset();
        impl_->started = false;
        throw;
    }
}

void LiveCaptureController::stop() {
    if (!impl_ ||
        impl_->terminal.load(std::memory_order_acquire)) {
        return;
    }
    impl_->stop_requested.store(true, std::memory_order_release);
    impl_->session->stop();
}

bool LiveCaptureController::terminal() const noexcept {
    return impl_ &&
           impl_->terminal.load(std::memory_order_acquire);
}

const LiveCaptureResult& LiveCaptureController::result()
    const noexcept {
    return impl_->result;
}

void LiveCaptureController::on_open() {
    if (impl_->stop_requested.load(std::memory_order_acquire)) {
        try {
            impl_->session->stop();
        } catch (...) {
            fail(LiveCaptureErrorCode::resume_notification_failure);
        }
    }
}

void LiveCaptureController::on_text_message(
    const WebSocketTextMessage& message) {
    if (impl_->terminal.load(std::memory_order_acquire) ||
        impl_->failure_latched) {
        return;
    }
    ++impl_->result.metrics.text_messages;

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
            return;
        }
        fail(LiveCaptureErrorCode::pipeline_failure);
        return;
    }

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
    if (impl_->writer->should_pause()) {
        pause_for_backpressure();
    }
}

void LiveCaptureController::on_control(
    const WebSocketControlFrame& frame) noexcept {
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
    const WebSocketSessionResult session_result) noexcept {
    if (impl_->terminal_started) {
        return;
    }
    impl_->terminal_started = true;
    boost::system::error_code ignored;
    impl_->backpressure_timer.cancel(ignored);
    impl_->pipeline->invalidate_all();
    impl_->result.session = session_result;
    impl_->result.stop_requested =
        impl_->stop_requested.load(std::memory_order_acquire);
    if (impl_->resume_post_failed.load(
            std::memory_order_acquire)) {
        impl_->result.error =
            LiveCaptureErrorCode::resume_notification_failure;
    } else if (!impl_->failure_latched) {
        const bool expected_stop_result =
            impl_->result.stop_requested &&
            (session_result.success() ||
             session_result.code ==
                 WebSocketSessionErrorCode::cancelled);
        if (!expected_stop_result) {
            impl_->result.error =
                LiveCaptureErrorCode::session_failure;
        }
    }
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
    if (impl_->reads_paused || impl_->failure_latched) {
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
            impl_->session->stop();
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
        impl_->session->resume_reads();
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
    try {
        impl_->session->pause_reads();
        impl_->session->stop();
    } catch (...) {
        impl_->resume_post_failed.store(
            true, std::memory_order_release);
        impl_->io_context.stop();
    }
}

}  // namespace hft
