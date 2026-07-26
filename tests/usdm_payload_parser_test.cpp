#include "test_framework.h"

#include "hft/spot_payload_parser.h"

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedUsdMJson {
public:
    explicit PaddedUsdMJson(const std::string_view json)
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

[[nodiscard]] SpotParseResult parse_usdm(
    SpotPayloadParser& parser,
    const SpotStreamKind kind,
    const std::string_view json,
    const std::string_view symbol = "BTCUSDT") {
    const PaddedUsdMJson padded{json};
    return parser.parse(
        PayloadVenue::usdm, kind, symbol, padded.view());
}

void expect_usdm_error(
    Context& context,
    SpotPayloadParser& parser,
    const SpotStreamKind kind,
    const std::string_view json,
    const SpotParseError expected_error,
    const SpotField expected_field,
    const std::string_view description) {
    const SpotParseResult result = parse_usdm(parser, kind, json);
    context.expect(
        !result,
        std::string{description} + " is rejected");
    context.expect(
        result.error == expected_error,
        std::string{description} + " reports " +
            std::string{to_string(expected_error)} + ", actual " +
            std::string{to_string(result.error)});
    context.expect(
        result.field == expected_field,
        std::string{description} + " identifies " +
            std::string{to_string(expected_field)});
}

void test_valid_usdm_depth(Context& context, SpotPayloadParser& parser) {
    constexpr std::string_view differential = R"({
        "e":"depthUpdate","E":1700000000100,"T":1700000000099,
        "s":"btcusdt","U":100,"u":105,"pu":99,
        "b":[["100","2"],["99","0"]],
        "a":[["101","3"]],
        "ps":"BTCUSDT","st":"TRADING"
    })";
    const SpotParseResult diff = parse_usdm(
        parser, SpotStreamKind::depth_diff, differential);
    context.expect(
        diff.has_value(), "valid USD-M differential is accepted");
    if (diff) {
        context.expect(
            diff.event.depth.has_event_time &&
                diff.event.depth.event_time_ms == 1'700'000'000'100ULL,
            "USD-M event time is retained");
        context.expect(
            diff.event.depth.has_transaction_time &&
                diff.event.depth.transaction_time_ms ==
                    1'700'000'000'099ULL,
            "USD-M transaction time is retained");
        context.expect(
            diff.event.depth.has_previous_update_id &&
                diff.event.depth.previous_update_id == 99U,
            "USD-M predecessor update ID is retained");
        context.expect(
            diff.event.depth.first_update_id == 100U &&
                diff.event.depth.final_update_id == 105U,
            "USD-M update range is retained");
        context.expect(
            diff.event.depth.bids.size == 2U &&
                diff.event.depth.bids[1U].quantity == 0,
            "USD-M differential permits zero-quantity removal");
    }

    constexpr std::string_view partial = R"({
        "e":"depthUpdate","E":1700000000200,"T":1700000000199,
        "s":"BTCUSDT","U":200,"u":205,"pu":199,
        "b":[["100","1"],["99","2"],["98","3"]],
        "a":[["101","4"],["102","5"]],
        "future":{"nested":[1e400]}
    })";
    const SpotParseResult refresh =
        parse_usdm(parser, SpotStreamKind::depth5, partial);
    context.expect(
        refresh.has_value(), "valid USD-M depth5 refresh is accepted");
    if (refresh) {
        context.expect(
            refresh.event.depth.has_event_time &&
                refresh.event.depth.has_transaction_time &&
                refresh.event.depth.has_previous_update_id,
            "USD-M depth5 retains all timing and chain metadata");
        context.expect(
            refresh.event.depth.first_update_id == 200U &&
                refresh.event.depth.final_update_id == 205U &&
                refresh.event.depth.previous_update_id == 199U,
            "USD-M depth5 retains U/u/pu");
        context.expect(
            refresh.event.depth.bids.size == 3U &&
                refresh.event.depth.asks.size == 2U,
            "USD-M depth5 uses replacement-sized b/a arrays");
    }
}

void test_usdm_trade_policy(Context& context, SpotPayloadParser& parser) {
    const SpotParseResult valid = parse_usdm(
        parser,
        SpotStreamKind::trade,
        R"({"e":"trade","s":"BTCUSDT","T":1,"p":"1","q":"2"})");
    context.expect(
        valid.has_value() &&
            valid.event.trade.event_type_matches &&
            valid.event.trade.symbol_matches,
        "USD-M trade remains audit-only with optional discriminators");

    const SpotParseResult mismatch = parse_usdm(
        parser,
        SpotStreamKind::trade,
        R"({"e":7,"s":"ETHUSDT","future":[1]})");
    context.expect(
        mismatch.has_value() &&
            !mismatch.event.trade.event_type_matches &&
            !mismatch.event.trade.symbol_matches,
        "USD-M trade discriminator mismatches remain auditable");
}

void test_usdm_required_fields(
    Context& context,
    SpotPayloadParser& parser) {
    struct MissingCase {
        std::string_view json;
        SpotField field;
        std::string_view description;
    };
    constexpr MissingCase cases[]{
        {
            R"({"E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})",
            SpotField::event_type,
            "missing USD-M event type",
        },
        {
            R"({"e":"depthUpdate","T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})",
            SpotField::event_time,
            "missing USD-M event time",
        },
        {
            R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})",
            SpotField::transaction_time,
            "missing USD-M transaction time",
        },
        {
            R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]})",
            SpotField::previous_update_id,
            "missing USD-M predecessor",
        },
        {
            R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"a":[]})",
            SpotField::bids,
            "missing USD-M bids",
        },
        {
            R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[]})",
            SpotField::asks,
            "missing USD-M asks",
        },
    };

    for (const SpotStreamKind kind :
         {SpotStreamKind::depth_diff, SpotStreamKind::depth5}) {
        for (const MissingCase& test_case : cases) {
            expect_usdm_error(
                context,
                parser,
                kind,
                test_case.json,
                SpotParseError::missing_field,
                test_case.field,
                test_case.description);
        }
    }

    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"lastUpdateId":1,"bids":[],"asks":[]})",
        SpotParseError::missing_field,
        SpotField::event_type,
        "Spot depth5 shape routed as USD-M");
}

void test_usdm_scalar_and_range_validation(
    Context& context,
    SpotPayloadParser& parser) {
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"T":"1","s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]})",
        SpotParseError::wrong_type,
        SpotField::transaction_time,
        "string USD-M transaction time");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":-1,"b":[],"a":[]})",
        SpotParseError::wrong_type,
        SpotField::previous_update_id,
        "negative USD-M predecessor");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":3,"u":2,"pu":1,"b":[],"a":[]})",
        SpotParseError::invalid_update_range,
        SpotField::first_update_id,
        "USD-M U greater than u");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth_diff,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":2,"b":[],"a":[]})",
        SpotParseError::invalid_update_range,
        SpotField::previous_update_id,
        "USD-M pu equal to u");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["100","0"]],"a":[]})",
        SpotParseError::invalid_quantity,
        SpotField::quantity,
        "zero USD-M depth5 quantity");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["99","1"],["100","1"]],"a":[]})",
        SpotParseError::invalid_level_order,
        SpotField::bids,
        "unordered USD-M depth5 bids");
    expect_usdm_error(
        context,
        parser,
        SpotStreamKind::depth5,
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[["101","1"]],"a":[["101","1"]]})",
        SpotParseError::crossed_book,
        SpotField::level,
        "crossed USD-M depth5");
}

}  // namespace

void run_usdm_payload_parser_tests(Context& context) {
    SpotPayloadParser parser;
    test_valid_usdm_depth(context, parser);
    test_usdm_trade_policy(context, parser);
    test_usdm_required_fields(context, parser);
    test_usdm_scalar_and_range_validation(context, parser);
}

}  // namespace hft::test
