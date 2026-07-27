#pragma once

#include "hft/event_processor.h"
#include "hft/live_envelope.h"
#include "hft/verified_websocket_session.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>

namespace hft {

enum class LivePipelineCreateError : std::uint8_t {
    none,
    invalid_subscription,
    state_initialization_failure,
    component_initialization_failure,
    allocation_failure,
};

enum class LivePipelineDisposition : std::uint8_t {
    not_processed,
    batch_ready,
    pre_audit_rejected,
    fatal_error,
};

enum class LivePipelineErrorCode : std::uint8_t {
    none,
    invalid_message_metadata,
    message_too_large,
    route_state_mismatch,
    event_processing_failed,
    invalid_batch_contract,
};

struct LivePipelineResult {
    LivePipelineDisposition disposition{
        LivePipelineDisposition::not_processed};
    LivePipelineErrorCode error{LivePipelineErrorCode::none};
    LiveEnvelopeErrorCode envelope_error{
        LiveEnvelopeErrorCode::none};
    LiveEnvelopeField envelope_field{LiveEnvelopeField::none};
    EventProcessResult event{};
    const LiveRoute* route{nullptr};
    std::size_t target_index{kNoSymbolIndex};

    [[nodiscard]] bool handled() const noexcept {
        return disposition == LivePipelineDisposition::batch_ready ||
               disposition ==
                   LivePipelineDisposition::pre_audit_rejected;
    }

    [[nodiscard]] bool has_batch() const noexcept {
        return disposition == LivePipelineDisposition::batch_ready;
    }

    [[nodiscard]] bool fatal() const noexcept {
        return disposition == LivePipelineDisposition::fatal_error;
    }
};

class LiveEventPipeline {
public:
    // The subscription must outlive the pipeline. Parser, state, and combined
    // message scratch storage are preallocated during creation. The caller
    // owns and reuses output; its bounded record buffers may grow while
    // formatting an exceptional row. Every input view is consumed
    // synchronously.
    [[nodiscard]] static std::unique_ptr<LiveEventPipeline> create(
        const LiveSubscription& subscription,
        LivePipelineCreateError& error) noexcept;

    LiveEventPipeline(const LiveEventPipeline&) = delete;
    LiveEventPipeline& operator=(const LiveEventPipeline&) = delete;
    LiveEventPipeline(LiveEventPipeline&&) = delete;
    LiveEventPipeline& operator=(LiveEventPipeline&&) = delete;

    ~LiveEventPipeline() noexcept;

    // output is cleared on entry. A batch_ready result identifies the stable
    // output target and leaves one complete audit-only or audit+book batch
    // for immediate publication to the writer boundary.
    [[nodiscard]] LivePipelineResult process(
        const WebSocketTextMessage& message,
        EventRowBatch& output) noexcept;

    void invalidate_all() noexcept;

    [[nodiscard]] std::size_t symbol_count() const noexcept;
    [[nodiscard]] const SymbolState* state(
        std::size_t symbol_index) const noexcept;

private:
    struct Impl;

    explicit LiveEventPipeline(
        const LiveSubscription& subscription);

    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const LivePipelineCreateError error) noexcept {
    switch (error) {
        case LivePipelineCreateError::none:
            return "none";
        case LivePipelineCreateError::invalid_subscription:
            return "invalid_subscription";
        case LivePipelineCreateError::state_initialization_failure:
            return "state_initialization_failure";
        case LivePipelineCreateError::component_initialization_failure:
            return "component_initialization_failure";
        case LivePipelineCreateError::allocation_failure:
            return "allocation_failure";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const LivePipelineDisposition disposition) noexcept {
    switch (disposition) {
        case LivePipelineDisposition::not_processed:
            return "not_processed";
        case LivePipelineDisposition::batch_ready:
            return "batch_ready";
        case LivePipelineDisposition::pre_audit_rejected:
            return "pre_audit_rejected";
        case LivePipelineDisposition::fatal_error:
            return "fatal_error";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const LivePipelineErrorCode error) noexcept {
    switch (error) {
        case LivePipelineErrorCode::none:
            return "none";
        case LivePipelineErrorCode::invalid_message_metadata:
            return "invalid_message_metadata";
        case LivePipelineErrorCode::message_too_large:
            return "message_too_large";
        case LivePipelineErrorCode::route_state_mismatch:
            return "route_state_mismatch";
        case LivePipelineErrorCode::event_processing_failed:
            return "event_processing_failed";
        case LivePipelineErrorCode::invalid_batch_contract:
            return "invalid_batch_contract";
    }
    return "unknown";
}

}  // namespace hft
