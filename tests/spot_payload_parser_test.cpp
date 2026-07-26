#include "test_framework.h"

#include "hft/spot_payload_parser.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedJson {
public:
    explicit PaddedJson(const std::string_view json)
        : storage_(json.size() + kJsonPaddingBytes, '\0'),
          size_(json.size()) {
        std::memcpy(storage_.data(), json.data(), json.size());
    }

    [[nodiscard]] PaddedJsonView view() const noexcept {
        return PaddedJsonView{
            storage_.data(), size_, storage_.size()};
    }

private:
    std::vector<char> storage_;
    std::size_t size_;
};

[[nodiscard]] SpotParseResult parse(
    SpotPayloadParser& parser,
    const SpotStreamKind kind,
    const std::string_view json,
    const std::string_view symbol = "BTCUSDT") {
    const PaddedJson padded{json};
    return parser.parse(kind, symbol, padded.view());
}

void expect_error(
    Context& context,
    SpotPayloadParser& parser,
    const SpotStreamKind kind,
    const std::string_view json,
    const SpotParseError expected_error,
    const SpotField expected_field,
    const std::string_view message) {
    const SpotParseResult result = parse(parser, kind, json);
    const std::string prefix{message};
    context.expect(!result, prefix + " is rejected");
    context.expect(
        result.error == expected_error,
        prefix + " reports " + std::string{to_string(expected_error)} +
            ", actual " + std::string{to_string(result.error)});
    context.expect(
        result.field == expected_field,
        prefix + " identifies the expected field");
}

[[nodiscard]] std::string differential_with_levels(
    const std::size_t level_count) {
    std::string json =
        R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[)";
    json.reserve(128U + (level_count * 24U));
    for (std::size_t index = 0; index < level_count; ++index) {
        if (index != 0U) {
            json.push_back(',');
        }
        json += "[\"";
        json += std::to_string(index + 1U);
        json += "\",\"1\"]";
    }
    json += R"(],"a":[]})";
    return json;
}

void test_valid_differential(Context& context, SpotPayloadParser& parser) {
    constexpr std::string_view json = R"({
        "a":[["101.00000000","2.5"]],
        "ignored":{"nested":[1,2,3]},
        "u":105,
        "s":"btcusdt",
        "b":[["100.00000000","1.25"],["99","0"]],
        "E":1700000000123,
        "U":100,
        "e":"depthUpdate"
    })";

    const SpotParseResult result =
        parse(parser, SpotStreamKind::depth_diff, json);
    context.expect(
        result.has_value(), "valid differential payload is accepted");
    if (!result) {
        return;
    }

    context.expect(
        result.event.kind == SpotStreamKind::depth_diff,
        "differential result retains its routed kind");
    context.expect(
        result.event.depth.has_event_time &&
            result.event.depth.event_time_ms == 1'700'000'000'123ULL,
        "differential event time is typed");
    context.expect(
        result.event.depth.first_update_id == 100U &&
            result.event.depth.final_update_id == 105U,
        "differential update range is typed");
    context.expect(
        result.event.depth.bids.size == 2U &&
            result.event.depth.asks.size == 1U,
        "differential bid and ask ranges are retained across field order");
    context.expect(
        result.event.depth.bids[0].price == 10'000'000'000LL &&
            result.event.depth.bids[0].quantity == 125'000'000LL,
        "differential bid values use fixed-point integers");
    context.expect(
        result.event.depth.bids[1].quantity == 0,
        "zero differential quantity is accepted as removal");
    context.expect(
        result.event.depth.asks[0].side == BookSide::ask,
        "differential ask side is typed");
}

void test_valid_partial_depth(Context& context, SpotPayloadParser& parser) {
    constexpr std::string_view json = R"({
        "lastUpdateId":160,
        "bids":[["100","2"],["99","3"],["98","4"],["97","5"],["96","6"]],
        "asks":[["101","4"],["102","5"],["103","6"],["104","7"],["105","8"]],
        "e":"ignored-for-route",
        "s":"WRONG"
    })";

    const SpotParseResult result =
        parse(parser, SpotStreamKind::depth5, json);
    context.expect(
        result.has_value(), "valid Spot depth5 payload is accepted");
    if (!result) {
        return;
    }

    context.expect(
        !result.event.depth.has_event_time,
        "Spot depth5 does not manufacture an exchange event time");
    context.expect(
        result.event.depth.first_update_id == 160U &&
            result.event.depth.final_update_id == 160U,
        "Spot depth5 exposes lastUpdateId as its refresh ID");
    context.expect(
        result.event.depth.bids.size == 5U &&
            result.event.depth.asks.size == 5U,
        "Spot depth5 accepts exactly five levels on each side");
}

void test_trade_audit_policy(Context& context, SpotPayloadParser& parser) {
    constexpr std::string_view valid = R"({
        "e":"trade","E":1672515782136,"s":"BTCUSDT","t":12345,
        "p":"0.001","q":"100","T":1672515782136,"m":true,"M":true
    })";
    const SpotParseResult accepted =
        parse(parser, SpotStreamKind::trade, valid);
    context.expect(
        accepted.has_value(), "valid trade object is auditable");
    context.expect(
        accepted.event.trade.event_type_present &&
            accepted.event.trade.event_type_matches &&
            accepted.event.trade.symbol_present &&
            accepted.event.trade.symbol_matches,
        "trade optional discriminator checks pass");

    constexpr std::string_view mismatch =
        R"({"e":7,"s":"ETHUSDT","p":false,"extra":[1]})";
    const SpotParseResult auditable =
        parse(parser, SpotStreamKind::trade, mismatch);
    context.expect(
        auditable.has_value(),
        "trade schema mismatch remains auditable under audit-only policy");
    context.expect(
        auditable.event.trade.event_type_present &&
            !auditable.event.trade.event_type_matches &&
            auditable.event.trade.symbol_present &&
            !auditable.event.trade.symbol_matches,
        "trade discriminator mismatches are surfaced without rejection");

    const SpotParseResult minimal =
        parse(parser, SpotStreamKind::trade, R"({"anything":1})");
    context.expect(
        minimal && !minimal.event.trade.event_type_present &&
            !minimal.event.trade.symbol_present,
        "a syntactically valid routed trade object needs no trade schema");

    expect_error(
        context,
        parser,
        SpotStreamKind::trade,
        R"({"e":tru})",
        SpotParseError::malformed_json,
        SpotField::root,
        "malformed optional trade discriminator");
}

void test_document_and_required_fields(
    Context& context,
    SpotPayloadParser& parser) {
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate")",
        SpotParseError::malformed_json,
        SpotField::root,
        "malformed JSON");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        "[]",
        SpotParseError::root_not_object,
        SpotField::root,
        "non-object root");
    struct MissingCase {
        SpotStreamKind kind;
        std::string_view json;
        SpotField field;
        std::string_view message;
    };
    constexpr std::array missing_cases{
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})",
            SpotField::event_type,
            "missing differential discriminator"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})",
            SpotField::event_time,
            "missing differential event time"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":1,"U":1,"u":1,"b":[],"a":[]})",
            SpotField::symbol,
            "missing differential symbol"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","u":1,"b":[],"a":[]})",
            SpotField::first_update_id,
            "missing differential first update ID"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"b":[],"a":[]})",
            SpotField::final_update_id,
            "missing differential final update ID"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"a":[]})",
            SpotField::bids,
            "missing differential bids"},
        MissingCase{
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[]})",
            SpotField::asks,
            "missing differential asks"},
        MissingCase{
            SpotStreamKind::depth5,
            R"({"bids":[],"asks":[]})",
            SpotField::last_update_id,
            "missing depth5 update ID"},
        MissingCase{
            SpotStreamKind::depth5,
            R"({"lastUpdateId":1,"asks":[]})",
            SpotField::bids,
            "missing depth5 bids"},
        MissingCase{
            SpotStreamKind::depth5,
            R"({"lastUpdateId":1,"bids":[]})",
            SpotField::asks,
            "missing depth5 asks"},
    };
    for (const MissingCase& test_case : missing_cases) {
        expect_error(
            context,
            parser,
            test_case.kind,
            test_case.json,
            SpotParseError::missing_field,
            test_case.field,
            test_case.message);
    }
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})",
        SpotParseError::duplicate_field,
        SpotField::event_type,
        "duplicate required field");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":7,"E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[],"tail":tru})",
        SpotParseError::malformed_json,
        SpotField::root,
        "syntax error after an earlier schema error");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","e":tru,"E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})",
        SpotParseError::malformed_json,
        SpotField::root,
        "syntax error in a duplicate field");
}

void test_scalar_validation(Context& context, SpotPayloadParser& parser) {
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"bookTicker","E":1,"s":"BTCUSDT","U":1,"u":1,"b":[],"a":[]})",
        SpotParseError::unexpected_event_type,
        SpotField::event_type,
        "wrong differential discriminator");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"s":"ETHUSDT","U":1,"u":1,"b":[],"a":[]})",
        SpotParseError::symbol_mismatch,
        SpotField::symbol,
        "payload symbol mismatch");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":2,"u":1,"b":[],"a":[]})",
        SpotParseError::invalid_update_range,
        SpotField::first_update_id,
        "reversed update range");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":-1,"u":1,"b":[],"a":[]})",
        SpotParseError::wrong_type,
        SpotField::first_update_id,
        "negative update ID");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1.0,"bids":[],"asks":[]})",
        SpotParseError::wrong_type,
        SpotField::last_update_id,
        "floating update ID");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":"1","bids":[],"asks":[]})",
        SpotParseError::wrong_type,
        SpotField::last_update_id,
        "string update ID");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":18446744073709551616,"u":1,"b":[],"a":[]})",
        SpotParseError::wrong_type,
        SpotField::first_update_id,
        "overflowing update ID");
}

void test_level_validation(Context& context, SpotPayloadParser& parser) {
    const auto diff = [](const std::string_view bids,
                         const std::string_view asks) {
        return std::string{
                   R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":1,"b":)"} +
               std::string{bids} + R"(,"a":)" + std::string{asks} + "}";
    };

    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1.000000001","1"]])", "[]"),
        SpotParseError::invalid_decimal,
        SpotField::price,
        "non-lossless price");
    const SpotParseResult decimal_detail = parse(
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1.000000001","1"]])", "[]"));
    context.expect(
        decimal_detail.decimal_error ==
            DecimalParseError::non_zero_discarded_digit,
        "level error retains the fixed-point rejection reason");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["0","1"]])", "[]"),
        SpotParseError::non_positive_price,
        SpotField::price,
        "zero price");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1"]])", "[]"),
        SpotParseError::invalid_level_shape,
        SpotField::level,
        "one-element level");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1","1","extra"]])", "[]"),
        SpotParseError::invalid_level_shape,
        SpotField::level,
        "three-element level");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([[1,"1"]])", "[]"),
        SpotParseError::wrong_type,
        SpotField::price,
        "numeric price token");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1.0","1"],["1.00000000","2"]])", "[]"),
        SpotParseError::duplicate_price,
        SpotField::price,
        "equivalent duplicate scaled price");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["bad","1"],tru])", "[]"),
        SpotParseError::malformed_json,
        SpotField::root,
        "syntax error after a bad decimal in one level array");

    const SpotParseResult same_cross_side = parse(
        parser,
        SpotStreamKind::depth_diff,
        diff(R"([["1","1"]])", R"([["1","1"]])"));
    context.expect(
        same_cross_side.has_value(),
        "same differential price on opposite sides is not a duplicate");

    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[["100","0"]],"asks":[]})",
        SpotParseError::invalid_quantity,
        SpotField::quantity,
        "zero depth5 quantity");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[["99","1"],["100","1"]],"asks":[]})",
        SpotParseError::invalid_level_order,
        SpotField::bids,
        "ascending depth5 bids");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[],"asks":[["102","1"],["101","1"]]})",
        SpotParseError::invalid_level_order,
        SpotField::asks,
        "descending depth5 asks");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[["101","1"]],"asks":[["101","1"]]})",
        SpotParseError::crossed_book,
        SpotField::level,
        "locked depth5 book");
    expect_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[["6","1"],["5","1"],["4","1"],["3","1"],["2","1"],["1","1"]],"asks":[]})",
        SpotParseError::too_many_levels,
        SpotField::bids,
        "sixth depth5 bid");
}

void test_capacity_limits(Context& context, SpotPayloadParser& parser) {
    const std::string exact = differential_with_levels(kMaxDepthUpdates);
    const SpotParseResult exact_result =
        parse(parser, SpotStreamKind::depth_diff, exact);
    context.expect(
        exact_result &&
            exact_result.event.depth.bids.size == kMaxDepthUpdates,
        "exact 16,384-update differential payload is accepted whole");

    const std::string over =
        differential_with_levels(kMaxDepthUpdates + 1U);
    expect_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        over,
        SpotParseError::too_many_levels,
        SpotField::bids,
        "16,385-update differential payload");

    constexpr char object[] = "{}";
    const SpotParseResult insufficient_padding = parser.parse(
        SpotStreamKind::trade,
        "BTCUSDT",
        PaddedJsonView{object, 2U, 2U});
    context.expect(
        insufficient_padding.error ==
            SpotParseError::invalid_input_buffer,
        "insufficient simdjson padding is rejected before parsing");

    const SpotParseResult too_large = parser.parse(
        SpotStreamKind::trade,
        "BTCUSDT",
        PaddedJsonView{
            object,
            kMaxPayloadBytes + 1U,
            kMaxPayloadBytes + 1U + kJsonPaddingBytes});
    context.expect(
        too_large.error == SpotParseError::payload_too_large,
        "payload above the 1 MiB limit is rejected before reading");
}

}  // namespace

void run_spot_payload_parser_tests(Context& context) {
    SpotPayloadParser parser;
    test_valid_differential(context, parser);
    test_valid_partial_depth(context, parser);
    test_trade_audit_policy(context, parser);
    test_document_and_required_fields(context, parser);
    test_scalar_validation(context, parser);
    test_level_validation(context, parser);
    test_capacity_limits(context, parser);
}

}  // namespace hft::test
