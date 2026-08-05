#include "hft/live_event_pipeline.h"

#include <array>
#include <cstring>
#include <new>
#include <optional>
#include <utility>

namespace hft {
namespace {

constexpr std::int32_t kNanosecondsPerSecond{1'000'000'000};

[[nodiscard]] LivePipelineResult fatal_result(
    const LivePipelineErrorCode error) noexcept {
    LivePipelineResult result;
    result.disposition = LivePipelineDisposition::fatal_error;
    result.error = error;
    return result;
}

}  // namespace

struct LiveEventPipeline::Impl {
    const LiveSubscription& subscription;
    std::array<std::optional<SymbolState>, kMaxConfiguredSymbols>
        states{};
    LiveEnvelopeParser envelope_parser{};
    EventProcessor event_processor{};
    std::unique_ptr<char[]> combined_message{
        std::make_unique<char[]>(
            kMaxPayloadBytes + kJsonPaddingBytes)};
    bool has_connection_metadata{false};
    std::uint64_t connection_epoch{0};
    std::uint64_t last_connection_sequence{0};

    explicit Impl(const LiveSubscription& configured_subscription)
        : subscription{configured_subscription} {}

    [[nodiscard]] bool valid_message_metadata(
        const WebSocketTextMessage& message) const noexcept {
        if (message.receive_timestamp.nanoseconds < 0 ||
            message.receive_timestamp.nanoseconds >=
                kNanosecondsPerSecond ||
            message.connection_sequence == 0U) {
            return false;
        }
        if (!has_connection_metadata) {
            // The controller owns epoch assignment. Earlier successful
            // sessions may disconnect before delivering a text message, so
            // the pipeline's first observed epoch need not be zero.
            return true;
        }
        if (message.connection_epoch < connection_epoch) {
            return false;
        }
        return message.connection_epoch != connection_epoch ||
               message.connection_sequence >
                   last_connection_sequence;
    }

    void invalidate_all() noexcept {
        for (std::size_t index = 0;
             index < subscription.symbol_count();
             ++index) {
            if (states[index]) {
                states[index]->invalidate();
            }
        }
    }

    [[nodiscard]] LivePipelineResult process(
        const WebSocketTextMessage& message,
        EventRowBatch& output) noexcept {
        output.clear();
        if (!valid_message_metadata(message)) {
            return fatal_result(
                LivePipelineErrorCode::invalid_message_metadata);
        }
        if (message.payload.size() > kMaxPayloadBytes) {
            return fatal_result(
                LivePipelineErrorCode::message_too_large);
        }
        if (message.payload.size() != 0U &&
            message.payload.data() == nullptr) {
            return fatal_result(
                LivePipelineErrorCode::invalid_message_metadata);
        }

        if (has_connection_metadata &&
            message.connection_epoch > connection_epoch) {
            invalidate_all();
        }
        has_connection_metadata = true;
        connection_epoch = message.connection_epoch;
        last_connection_sequence = message.connection_sequence;

        if (!message.payload.empty()) {
            std::memcpy(
                combined_message.get(),
                message.payload.data(),
                message.payload.size());
        }
        std::memset(
            combined_message.get() + message.payload.size(),
            0,
            kJsonPaddingBytes);
        const LiveEnvelopeResult envelope =
            envelope_parser.parse(
                PaddedJsonView{
                    combined_message.get(),
                    message.payload.size(),
                    kMaxPayloadBytes + kJsonPaddingBytes},
                subscription);

        LivePipelineResult result;
        result.route = envelope.route;
        result.envelope_error = envelope.error;
        result.envelope_field = envelope.field;
        if (!envelope.success()) {
            if (envelope.route != nullptr &&
                envelope.route->stream_kind ==
                    SpotStreamKind::depth_diff &&
                envelope.route->symbol_index <
                    subscription.symbol_count() &&
                states[envelope.route->symbol_index]) {
                states[envelope.route->symbol_index]
                    ->invalidate();
            }
            result.disposition =
                LivePipelineDisposition::pre_audit_rejected;
            if (envelope.route != nullptr) {
                result.target_index =
                    envelope.route->symbol_index;
            }
            return result;
        }

        if (envelope.route->symbol_index >=
                subscription.symbol_count() ||
            !states[envelope.route->symbol_index]) {
            return fatal_result(
                LivePipelineErrorCode::route_state_mismatch);
        }
        result.target_index = envelope.route->symbol_index;
        const EventContext context{
            message.receive_timestamp,
            subscription.venue(),
            envelope.route->stream_kind,
            0U,
            message.connection_epoch,
            message.connection_sequence,
            envelope.route->normalized_symbol,
        };
        result.event = event_processor.process(
            *states[result.target_index],
            context,
            envelope.payload,
            EventOutputMode::live_capture,
            output);
        if (!result.event.success()) {
            output.clear();
            result.disposition =
                LivePipelineDisposition::fatal_error;
            result.error =
                LivePipelineErrorCode::event_processing_failed;
            return result;
        }
        if (result.event.status ==
            EventProcessStatus::pre_audit_rejected) {
            output.clear();
            result.disposition =
                LivePipelineDisposition::pre_audit_rejected;
            return result;
        }
        if (!output.has_audit_row ||
            output.audit_row.empty() ||
            (output.has_order_book_row ==
             output.order_book_row.empty())) {
            output.clear();
            result.disposition =
                LivePipelineDisposition::fatal_error;
            result.error =
                LivePipelineErrorCode::invalid_batch_contract;
            return result;
        }
        result.disposition =
            LivePipelineDisposition::batch_ready;
        return result;
    }
};

std::unique_ptr<LiveEventPipeline> LiveEventPipeline::create(
    const LiveSubscription& subscription,
    LivePipelineCreateError& error) noexcept {
    error = LivePipelineCreateError::none;
    if (subscription.symbol_count() == 0U ||
        subscription.symbol_count() > kMaxConfiguredSymbols ||
        subscription.route_count() !=
            subscription.symbol_count() * kStreamsPerSymbol) {
        error = LivePipelineCreateError::invalid_subscription;
        return nullptr;
    }
    try {
        std::unique_ptr<LiveEventPipeline> pipeline{
            new LiveEventPipeline{subscription}};
        for (std::size_t index = 0;
             index < subscription.symbol_count();
             ++index) {
            pipeline->impl_->states[index] =
                SymbolState::create(
                    subscription.venue(),
                    subscription.symbol(index));
            if (!pipeline->impl_->states[index]) {
                error =
                    LivePipelineCreateError::
                        state_initialization_failure;
                return nullptr;
            }
        }
        return pipeline;
    } catch (const std::bad_alloc&) {
        error = LivePipelineCreateError::allocation_failure;
        return nullptr;
    } catch (...) {
        error =
            LivePipelineCreateError::
                component_initialization_failure;
        return nullptr;
    }
}

LiveEventPipeline::LiveEventPipeline(
    const LiveSubscription& subscription)
    : impl_{std::make_unique<Impl>(subscription)} {}

LiveEventPipeline::~LiveEventPipeline() noexcept = default;

LivePipelineResult LiveEventPipeline::process(
    const WebSocketTextMessage& message,
    EventRowBatch& output) noexcept {
    if (!impl_) {
        output.clear();
        return fatal_result(
            LivePipelineErrorCode::route_state_mismatch);
    }
    return impl_->process(message, output);
}

void LiveEventPipeline::invalidate_all() noexcept {
    if (impl_) {
        impl_->invalidate_all();
    }
}

std::size_t LiveEventPipeline::symbol_count() const noexcept {
    return impl_ ? impl_->subscription.symbol_count() : 0U;
}

const SymbolState* LiveEventPipeline::state(
    const std::size_t symbol_index) const noexcept {
    if (!impl_ ||
        symbol_index >= impl_->subscription.symbol_count() ||
        !impl_->states[symbol_index]) {
        return nullptr;
    }
    return &*impl_->states[symbol_index];
}

}  // namespace hft
