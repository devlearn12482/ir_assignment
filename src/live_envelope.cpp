#include "hft/live_envelope.h"

#include "json_validation.h"

#include <simdjson.h>

#include <cstring>
#include <stdexcept>
#include <utility>

namespace hft {
namespace {

constexpr std::size_t kEnvelopeParserDepth{
    kMaxJsonNestingDepth + 8U};
constexpr std::size_t kEnvelopeChildContainerDepth{2U};

static_assert(kJsonPaddingBytes == simdjson::SIMDJSON_PADDING);
static_assert(kEnvelopeParserDepth > kMaxJsonNestingDepth);

[[nodiscard]] LiveEnvelopeErrorCode classify_json_error(
    const simdjson::error_code error) noexcept {
    return error == simdjson::DEPTH_ERROR
               ? LiveEnvelopeErrorCode::json_nesting_too_deep
               : LiveEnvelopeErrorCode::malformed_json;
}

[[nodiscard]] LiveEnvelopeResult fail(
    const LiveEnvelopeErrorCode error,
    const LiveEnvelopeField field,
    const LiveRoute* const route = nullptr) noexcept {
    LiveEnvelopeResult result;
    result.route = route;
    result.error = error;
    result.field = field;
    return result;
}

}  // namespace

struct LiveEnvelopeParser::Impl {
    simdjson::ondemand::parser parser{};
    std::unique_ptr<char[]> payload{
        std::make_unique<char[]>(
            kMaxPayloadBytes + kJsonPaddingBytes)};

    Impl() {
        const simdjson::error_code error =
            parser.allocate(kMaxPayloadBytes, kEnvelopeParserDepth);
        if (error != simdjson::SUCCESS) {
            throw std::runtime_error{
                "unable to preallocate live envelope parser"};
        }
    }

    [[nodiscard]] LiveEnvelopeResult parse(
        const PaddedJsonView combined_message,
        const LiveSubscription& subscription) noexcept {
        simdjson::ondemand::document document;
        simdjson::error_code error =
            parser
                .iterate(
                    combined_message.data,
                    combined_message.size,
                    combined_message.capacity)
                .get(document);
        if (error != simdjson::SUCCESS) {
            return fail(
                classify_json_error(error),
                LiveEnvelopeField::root);
        }

        simdjson::ondemand::json_type root_type;
        error = document.type().get(root_type);
        if (error != simdjson::SUCCESS) {
            return fail(
                classify_json_error(error),
                LiveEnvelopeField::root);
        }
        if (root_type != simdjson::ondemand::json_type::object) {
            error = detail::validate_non_object_document(
                document,
                root_type,
                std::string_view{
                    combined_message.data, combined_message.size},
                kEnvelopeChildContainerDepth);
            return fail(
                error == simdjson::SUCCESS
                    ? LiveEnvelopeErrorCode::root_not_object
                    : classify_json_error(error),
                LiveEnvelopeField::root);
        }

        simdjson::ondemand::object object;
        error = document.get_object().get(object);
        if (error != simdjson::SUCCESS) {
            return fail(
                classify_json_error(error),
                LiveEnvelopeField::root);
        }

        bool stream_seen = false;
        bool stream_valid = false;
        bool stream_ambiguous = false;
        bool data_seen = false;
        std::string_view stream_name;
        std::string_view raw_data;
        LiveEnvelopeErrorCode semantic_error{
            LiveEnvelopeErrorCode::none};
        LiveEnvelopeField semantic_field{LiveEnvelopeField::none};
        const auto record_semantic_error =
            [&](const LiveEnvelopeErrorCode code,
                const LiveEnvelopeField field) noexcept {
                if (semantic_error ==
                    LiveEnvelopeErrorCode::none) {
                    semantic_error = code;
                    semantic_field = field;
                }
            };
        const auto known_route = [&]() noexcept
            -> const LiveRoute* {
            if (!stream_valid || stream_ambiguous) {
                return nullptr;
            }
            return subscription.find_route(stream_name).route;
        };

        for (auto field_result : object) {
            simdjson::ondemand::field field;
            error = std::move(field_result).get(field);
            if (error != simdjson::SUCCESS) {
                return fail(
                    classify_json_error(error),
                    LiveEnvelopeField::root,
                    known_route());
            }
            std::string_view key;
            error = field.unescaped_key(false).get(key);
            if (error != simdjson::SUCCESS) {
                return fail(
                    classify_json_error(error),
                    LiveEnvelopeField::root,
                    known_route());
            }
            simdjson::ondemand::value& value = field.value();
            if (key == "stream") {
                if (stream_seen) {
                    stream_ambiguous = true;
                    record_semantic_error(
                        LiveEnvelopeErrorCode::duplicate_stream,
                        LiveEnvelopeField::stream);
                    error = detail::validate_json_value(
                        value, kEnvelopeChildContainerDepth);
                    if (error != simdjson::SUCCESS) {
                        return fail(
                            classify_json_error(error),
                            LiveEnvelopeField::stream);
                    }
                    continue;
                }
                stream_seen = true;
                simdjson::ondemand::json_type type;
                error = value.type().get(type);
                if (error != simdjson::SUCCESS) {
                    return fail(
                        classify_json_error(error),
                        LiveEnvelopeField::stream,
                        known_route());
                }
                if (type !=
                    simdjson::ondemand::json_type::string) {
                    error = detail::validate_json_value(
                        value, kEnvelopeChildContainerDepth);
                    if (error != simdjson::SUCCESS) {
                        return fail(
                            classify_json_error(error),
                            LiveEnvelopeField::stream);
                    }
                    record_semantic_error(
                        LiveEnvelopeErrorCode::stream_wrong_type,
                        LiveEnvelopeField::stream);
                    continue;
                }
                error = value.get_string().get(stream_name);
                if (error != simdjson::SUCCESS) {
                    return fail(
                        classify_json_error(error),
                        LiveEnvelopeField::stream);
                }
                stream_valid = true;
            } else if (key == "data") {
                if (data_seen) {
                    record_semantic_error(
                        LiveEnvelopeErrorCode::duplicate_data,
                        LiveEnvelopeField::data);
                    error = detail::validate_json_value(
                        value, kEnvelopeChildContainerDepth);
                    if (error != simdjson::SUCCESS) {
                        return fail(
                            classify_json_error(error),
                            LiveEnvelopeField::data,
                            known_route());
                    }
                    continue;
                }
                data_seen = true;
                simdjson::ondemand::json_type type;
                error = value.type().get(type);
                if (error != simdjson::SUCCESS) {
                    return fail(
                        classify_json_error(error),
                        LiveEnvelopeField::data,
                        known_route());
                }
                if (type !=
                    simdjson::ondemand::json_type::object) {
                    error = detail::validate_json_value(
                        value, kEnvelopeChildContainerDepth);
                    if (error != simdjson::SUCCESS) {
                        return fail(
                            classify_json_error(error),
                            LiveEnvelopeField::data,
                            known_route());
                    }
                    record_semantic_error(
                        LiveEnvelopeErrorCode::data_not_object,
                        LiveEnvelopeField::data);
                    continue;
                }
                error = value.raw_json().get(raw_data);
                if (error != simdjson::SUCCESS) {
                    return fail(
                        classify_json_error(error),
                        LiveEnvelopeField::data,
                        known_route());
                }
            } else {
                error = detail::validate_json_value(
                    value, kEnvelopeChildContainerDepth);
                if (error != simdjson::SUCCESS) {
                    return fail(
                        classify_json_error(error),
                        LiveEnvelopeField::unknown,
                        known_route());
                }
            }
        }
        if (!stream_seen) {
            return fail(
                LiveEnvelopeErrorCode::missing_stream,
                LiveEnvelopeField::stream);
        }

        const RouteLookupResult route =
            subscription.find_route(stream_name);
        const LiveRoute* const safe_route =
            stream_valid && !stream_ambiguous
                ? route.route
                : nullptr;
        if (semantic_error != LiveEnvelopeErrorCode::none) {
            return fail(
                semantic_error,
                semantic_field,
                safe_route);
        }
        if (!data_seen) {
            return fail(
                LiveEnvelopeErrorCode::missing_data,
                LiveEnvelopeField::data,
                safe_route);
        }
        if (!route.success()) {
            return fail(
                route.error ==
                        RouteLookupError::stream_name_too_long
                    ? LiveEnvelopeErrorCode::stream_name_too_long
                    : LiveEnvelopeErrorCode::unknown_stream,
                LiveEnvelopeField::stream);
        }

        std::size_t payload_size = 0;
        error = simdjson::minify(
            raw_data.data(),
            raw_data.size(),
            payload.get(),
            payload_size);
        if (error != simdjson::SUCCESS) {
            return fail(
                LiveEnvelopeErrorCode::payload_minify_failed,
                LiveEnvelopeField::data,
                route.route);
        }
        std::memset(
            payload.get() + payload_size,
            0,
            kJsonPaddingBytes);

        LiveEnvelopeResult result;
        result.route = route.route;
        result.payload = PaddedJsonView{
            payload.get(),
            payload_size,
            kMaxPayloadBytes + kJsonPaddingBytes};
        return result;
    }
};

LiveEnvelopeParser::LiveEnvelopeParser()
    : impl_{std::make_unique<Impl>()} {}

LiveEnvelopeParser::LiveEnvelopeParser(
    LiveEnvelopeParser&&) noexcept = default;

LiveEnvelopeParser& LiveEnvelopeParser::operator=(
    LiveEnvelopeParser&&) noexcept = default;

LiveEnvelopeParser::~LiveEnvelopeParser() = default;

LiveEnvelopeResult LiveEnvelopeParser::parse(
    const PaddedJsonView combined_message,
    const LiveSubscription& subscription) noexcept {
    if (impl_ == nullptr || combined_message.data == nullptr ||
        combined_message.capacity < combined_message.size ||
        combined_message.capacity - combined_message.size <
            kJsonPaddingBytes) {
        return fail(
            LiveEnvelopeErrorCode::invalid_input_buffer,
            LiveEnvelopeField::root);
    }
    if (combined_message.size > kMaxPayloadBytes) {
        return fail(
            LiveEnvelopeErrorCode::message_too_large,
            LiveEnvelopeField::root);
    }
    return impl_->parse(combined_message, subscription);
}

}  // namespace hft
