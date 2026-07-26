#include "test_framework.h"

#include "hft/live_envelope.h"
#include "hft/spot_payload_parser.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedMessage {
public:
    explicit PaddedMessage(const std::string_view text)
        : storage_(text.size() + kJsonPaddingBytes, '\0'),
          size_{text.size()} {
        std::copy(text.begin(), text.end(), storage_.begin());
    }

    [[nodiscard]] PaddedJsonView view() const noexcept {
        return PaddedJsonView{
            storage_.data(), size_, storage_.size()};
    }

private:
    std::vector<char> storage_{};
    std::size_t size_{0};
};

[[nodiscard]] std::unique_ptr<LiveSubscription>
make_subscription(Context& context) {
    constexpr std::array<std::string_view, 1U> symbols{"BTCUSDT"};
    SubscriptionError error;
    std::unique_ptr<LiveSubscription> subscription =
        LiveSubscription::create(
            PayloadVenue::spot,
            symbols.data(),
            symbols.size(),
            error);
    context.expect(
        subscription != nullptr,
        "envelope test subscription is created");
    return subscription;
}

void test_byte_faithful_extraction(Context& context) {
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(context);
    if (!subscription) {
        return;
    }
    const PaddedMessage message{R"json(
        {
          "extra": {"nested": [true, null, 1e400]},
          "data": {
            "z": 1.2300e+02,
            "text": "A\\n\\u0042\\\"",
            "array": [3, 2, 1],
            "a": -0.00000000
          },
          "stream": "btcusdt@trade"
        }
    )json"};
    LiveEnvelopeParser parser;
    const LiveEnvelopeResult result =
        parser.parse(message.view(), *subscription);
    constexpr std::string_view expected =
        R"json({"z":1.2300e+02,"text":"A\\n\\u0042\\\"","array":[3,2,1],"a":-0.00000000})json";
    context.expect(
        result.success() &&
            result.route->stream_kind == SpotStreamKind::trade &&
            result.route->normalized_symbol == "BTCUSDT" &&
            std::string_view{
                result.payload.data, result.payload.size} == expected,
        "data-before-stream extraction preserves JSON lexemes and order");
}

void test_payload_is_ready_for_shared_parser(Context& context) {
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(context);
    if (!subscription) {
        return;
    }
    const PaddedMessage message{
        R"json({"stream":"btcusdt@depth5@100ms","data":{
          "lastUpdateId":100,
          "bids":[["100.0","2.5"]],
          "asks":[["101.0","3.5"]]
        }})json"};
    LiveEnvelopeParser envelope_parser;
    const LiveEnvelopeResult envelope =
        envelope_parser.parse(message.view(), *subscription);
    context.expect(
        envelope.success(),
        "depth5 envelope extraction succeeds");
    if (!envelope.success()) {
        return;
    }
    SpotPayloadParser payload_parser;
    const SpotParseResult payload = payload_parser.parse(
        PayloadVenue::spot,
        envelope.route->stream_kind,
        envelope.route->normalized_symbol,
        envelope.payload);
    context.expect(
        payload.has_value() &&
            payload.event.depth.final_update_id == 100U,
        "extracted buffer is padded and accepted by shared parser");
}

void test_route_and_required_field_errors(Context& context) {
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(context);
    if (!subscription) {
        return;
    }
    LiveEnvelopeParser parser;

    const PaddedMessage unknown{
        R"json({"stream":"ethusdt@trade","data":{}})json"};
    context.expect(
        parser.parse(unknown.view(), *subscription).error ==
            LiveEnvelopeErrorCode::unknown_stream,
        "unsubscribed stream is rejected");

    const std::string exact_name(
        kMaxEnvelopeStreamNameBytes, 'a');
    const std::string long_name(
        kMaxEnvelopeStreamNameBytes + 1U, 'a');
    const PaddedMessage exact{
        "{\"stream\":\"" + exact_name + "\",\"data\":{}}"};
    const PaddedMessage too_long{
        "{\"stream\":\"" + long_name + "\",\"data\":{}}"};
    context.expect(
        parser.parse(exact.view(), *subscription).error ==
            LiveEnvelopeErrorCode::unknown_stream,
        "128-byte envelope stream reaches route lookup");
    context.expect(
        parser.parse(too_long.view(), *subscription).error ==
            LiveEnvelopeErrorCode::stream_name_too_long,
        "129-byte envelope stream is rejected at its boundary");

    const PaddedMessage missing_stream{R"json({"data":{}})json"};
    const PaddedMessage missing_data{
        R"json({"stream":"btcusdt@trade"})json"};
    const PaddedMessage duplicate_stream{
        R"json({"stream":"btcusdt@trade","stream":"btcusdt@trade","data":{}})json"};
    const PaddedMessage duplicate_data{
        R"json({"stream":"btcusdt@trade","data":{},"data":{}})json"};
    context.expect(
        parser.parse(missing_stream.view(), *subscription).error ==
            LiveEnvelopeErrorCode::missing_stream,
        "missing stream is classified");
    context.expect(
        parser.parse(missing_data.view(), *subscription).error ==
            LiveEnvelopeErrorCode::missing_data,
        "missing data is classified");
    context.expect(
        parser.parse(duplicate_stream.view(), *subscription).error ==
            LiveEnvelopeErrorCode::duplicate_stream,
        "duplicate stream is rejected");
    context.expect(
        parser.parse(duplicate_data.view(), *subscription).error ==
            LiveEnvelopeErrorCode::duplicate_data,
        "duplicate data is rejected");
}

void test_type_syntax_and_reuse_errors(Context& context) {
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(context);
    if (!subscription) {
        return;
    }
    LiveEnvelopeParser parser;
    const PaddedMessage root_array{"[]"};
    const PaddedMessage malformed_root{"[}"};
    const PaddedMessage stream_number{
        R"json({"stream":7,"data":{}})json"};
    const PaddedMessage data_array{
        R"json({"stream":"btcusdt@trade","data":[]})json"};
    const PaddedMessage malformed{
        R"json({"stream":"btcusdt@trade","data":{"x":]}})json"};
    const LiveEnvelopeResult malformed_result =
        parser.parse(malformed.view(), *subscription);
    context.expect(
        parser.parse(root_array.view(), *subscription).error ==
            LiveEnvelopeErrorCode::root_not_object,
        "non-object envelope root is rejected");
    context.expect(
        parser.parse(malformed_root.view(), *subscription).error ==
            LiveEnvelopeErrorCode::malformed_json,
        "malformed non-object root is not misclassified by type");
    context.expect(
        parser.parse(stream_number.view(), *subscription).error ==
            LiveEnvelopeErrorCode::stream_wrong_type,
        "non-string stream is rejected");
    context.expect(
        parser.parse(data_array.view(), *subscription).error ==
            LiveEnvelopeErrorCode::data_not_object,
        "non-object data is rejected");
    context.expect(
        malformed_result.success(),
        "lazy envelope extraction locates malformed inner object");
    SpotPayloadParser payload_parser;
    const SpotParseResult malformed_payload = payload_parser.parse(
        PayloadVenue::spot,
        SpotStreamKind::trade,
        "BTCUSDT",
        malformed_result.payload);
    context.expect(
        malformed_payload.error == SpotParseError::malformed_json,
        "shared payload parser rejects malformed inner object pre-audit");

    const PaddedMessage valid{
        R"json({"stream":"btcusdt@trade","data":{"e":"trade"}})json"};
    context.expect(
        parser.parse(valid.view(), *subscription).success(),
        "parser is reusable after classified failures");
}

[[nodiscard]] std::string nested_array(const std::size_t depth) {
    std::string result;
    result.reserve(depth * 2U + 1U);
    result.append(depth, '[');
    result.push_back('0');
    result.append(depth, ']');
    return result;
}

void test_depth_and_input_boundaries(Context& context) {
    const std::unique_ptr<LiveSubscription> subscription =
        make_subscription(context);
    if (!subscription) {
        return;
    }
    LiveEnvelopeParser parser;
    const PaddedMessage depth64{
        "{\"stream\":\"btcusdt@trade\",\"unknown\":" +
        nested_array(63U) + ",\"data\":{}}"};
    const PaddedMessage depth65{
        "{\"stream\":\"btcusdt@trade\",\"unknown\":" +
        nested_array(64U) + ",\"data\":{}}"};
    context.expect(
        parser.parse(depth64.view(), *subscription).success(),
        "64 open envelope containers are accepted");
    context.expect(
        parser.parse(depth65.view(), *subscription).error ==
            LiveEnvelopeErrorCode::json_nesting_too_deep,
        "65 open envelope containers are rejected");

    const PaddedMessage valid{
        R"json({"stream":"btcusdt@trade","data":{}})json"};
    PaddedJsonView insufficient = valid.view();
    insufficient.capacity = insufficient.size;
    context.expect(
        parser.parse(insufficient, *subscription).error ==
            LiveEnvelopeErrorCode::invalid_input_buffer,
        "missing parser padding is rejected");

    constexpr std::string_view exact_prefix{
        R"json({"stream":"btcusdt@trade","data":{"x":")json"};
    constexpr std::string_view exact_suffix{R"json("}})json"};
    std::string exact_message{exact_prefix};
    exact_message.append(
        kMaxPayloadBytes - exact_prefix.size() - exact_suffix.size(),
        'a');
    exact_message.append(exact_suffix);
    const PaddedMessage exact_limit{exact_message};
    context.expect(
        parser.parse(exact_limit.view(), *subscription).success(),
        "exact 1 MiB logical message is accepted without growth");

    std::vector<char> oversized(
        kMaxPayloadBytes + kJsonPaddingBytes + 1U, ' ');
    context.expect(
        parser
                .parse(
                    PaddedJsonView{
                        oversized.data(),
                        kMaxPayloadBytes + 1U,
                        oversized.size()},
                    *subscription)
                .error == LiveEnvelopeErrorCode::message_too_large,
        "one-over logical message limit is rejected before parsing");
}

}  // namespace

void run_live_envelope_tests(Context& context) {
    test_byte_faithful_extraction(context);
    test_payload_is_ready_for_shared_parser(context);
    test_route_and_required_field_errors(context);
    test_type_syntax_and_reuse_errors(context);
    test_depth_and_input_boundaries(context);
}

}  // namespace hft::test
