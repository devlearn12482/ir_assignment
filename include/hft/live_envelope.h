#pragma once

#include "hft/live_subscription.h"

#include <cstdint>
#include <memory>
#include <string_view>

namespace hft {

enum class LiveEnvelopeErrorCode : std::uint8_t {
    none,
    invalid_input_buffer,
    message_too_large,
    json_nesting_too_deep,
    malformed_json,
    root_not_object,
    missing_stream,
    missing_data,
    duplicate_stream,
    duplicate_data,
    stream_wrong_type,
    stream_name_too_long,
    unknown_stream,
    data_not_object,
    payload_minify_failed,
};

enum class LiveEnvelopeField : std::uint8_t {
    none,
    root,
    stream,
    data,
    unknown,
};

struct LiveEnvelopeResult {
    const LiveRoute* route{nullptr};
    PaddedJsonView payload{};
    LiveEnvelopeErrorCode error{LiveEnvelopeErrorCode::none};
    LiveEnvelopeField field{LiveEnvelopeField::none};

    [[nodiscard]] bool success() const noexcept {
        return error == LiveEnvelopeErrorCode::none &&
               route != nullptr && payload.data != nullptr;
    }
};

class LiveEnvelopeParser {
public:
    LiveEnvelopeParser();
    LiveEnvelopeParser(const LiveEnvelopeParser&) = delete;
    LiveEnvelopeParser& operator=(const LiveEnvelopeParser&) = delete;
    LiveEnvelopeParser(LiveEnvelopeParser&&) noexcept;
    LiveEnvelopeParser& operator=(LiveEnvelopeParser&&) noexcept;
    ~LiveEnvelopeParser();

    // Returned route and payload views remain valid until the subscription
    // or this parser is destroyed, respectively. No input view escapes.
    // The shared payload parser remains the authority for complete inner
    // object syntax; lexical minification alone is not a JSON validator.
    [[nodiscard]] LiveEnvelopeResult parse(
        PaddedJsonView combined_message,
        const LiveSubscription& subscription) noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

[[nodiscard]] constexpr std::string_view to_string(
    const LiveEnvelopeErrorCode code) noexcept {
    switch (code) {
        case LiveEnvelopeErrorCode::none:
            return "none";
        case LiveEnvelopeErrorCode::invalid_input_buffer:
            return "invalid_input_buffer";
        case LiveEnvelopeErrorCode::message_too_large:
            return "message_too_large";
        case LiveEnvelopeErrorCode::json_nesting_too_deep:
            return "json_nesting_too_deep";
        case LiveEnvelopeErrorCode::malformed_json:
            return "malformed_json";
        case LiveEnvelopeErrorCode::root_not_object:
            return "root_not_object";
        case LiveEnvelopeErrorCode::missing_stream:
            return "missing_stream";
        case LiveEnvelopeErrorCode::missing_data:
            return "missing_data";
        case LiveEnvelopeErrorCode::duplicate_stream:
            return "duplicate_stream";
        case LiveEnvelopeErrorCode::duplicate_data:
            return "duplicate_data";
        case LiveEnvelopeErrorCode::stream_wrong_type:
            return "stream_wrong_type";
        case LiveEnvelopeErrorCode::stream_name_too_long:
            return "stream_name_too_long";
        case LiveEnvelopeErrorCode::unknown_stream:
            return "unknown_stream";
        case LiveEnvelopeErrorCode::data_not_object:
            return "data_not_object";
        case LiveEnvelopeErrorCode::payload_minify_failed:
            return "payload_minify_failed";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const LiveEnvelopeField field) noexcept {
    switch (field) {
        case LiveEnvelopeField::none:
            return "none";
        case LiveEnvelopeField::root:
            return "root";
        case LiveEnvelopeField::stream:
            return "stream";
        case LiveEnvelopeField::data:
            return "data";
        case LiveEnvelopeField::unknown:
            return "unknown";
    }
    return "unknown";
}

}  // namespace hft
