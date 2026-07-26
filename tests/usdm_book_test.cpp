#include "test_framework.h"

#include "hft/spot_book.h"

#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedUsdMBookJson {
public:
    explicit PaddedUsdMBookJson(const std::string_view json)
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

[[nodiscard]] std::string usdm_depth(
    const std::uint64_t first_update_id,
    const std::uint64_t final_update_id,
    const std::uint64_t previous_update_id,
    const std::string_view bids,
    const std::string_view asks) {
    return R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":)" +
           std::to_string(first_update_id) + R"(,"u":)" +
           std::to_string(final_update_id) + R"(,"pu":)" +
           std::to_string(previous_update_id) + R"(,"b":)" +
           std::string{bids} + R"(,"a":)" + std::string{asks} + "}";
}

[[nodiscard]] SpotBookApplyResult parse_and_apply_usdm(
    Context& context,
    SpotPayloadParser& parser,
    UsdMBookState& book,
    const SpotStreamKind kind,
    const std::string& json,
    const std::string_view description) {
    const PaddedUsdMBookJson padded{json};
    const SpotParseResult parsed = parser.parse(
        PayloadVenue::usdm,
        kind,
        "BTCUSDT",
        padded.view());
    context.expect(
        parsed.has_value(),
        std::string{description} + " parses before application");
    if (!parsed) {
        return {};
    }
    return kind == SpotStreamKind::depth5
               ? book.apply_refresh(parsed.event.depth)
               : book.apply_diff(parsed.event.depth);
}

void test_usdm_bridge_and_chain(Context& context) {
    SpotPayloadParser parser;
    UsdMBookState book;

    const SpotBookApplyResult refresh = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            90U,
            100U,
            89U,
            R"([["100","1"],["99","1"]])",
            R"([["101","1"],["102","1"]])"),
        "USD-M seed refresh");
    context.expect(
        refresh.status == SpotBookApplyStatus::applied_refresh &&
            book.valid() && book.last_update_id() == 100U,
        "USD-M refresh establishes valid replacement state");
    context.expect(
        !book.diff_chain_established(),
        "USD-M refresh leaves independent diff stream unbridged");

    const SpotBookApplyResult pre_bridge_stale = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(80U, 100U, 7U, R"([["110","1"]])", "[]"),
        "pre-bridge stale diff");
    context.expect(
        pre_bridge_stale.status == SpotBookApplyStatus::stale_diff &&
            book.valid() && !book.diff_chain_established(),
        "fully stale pre-bridge diff ignores unrelated pu");

    const SpotBookApplyResult bridge = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(99U, 102U, 7U, R"([["100","2"]])", "[]"),
        "USD-M bridge diff");
    context.expect(
        bridge.status == SpotBookApplyStatus::applied_diff &&
            book.diff_chain_established() &&
            book.previous_diff_update_id() == 102U &&
            book.last_update_id() == 102U,
        "first applicable diff bridges by range without comparing pu");

    const SpotBookApplyResult chained = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(500U, 501U, 102U, R"([["100","3"]])", "[]"),
        "strictly chained diff");
    context.expect(
        chained.status == SpotBookApplyStatus::applied_diff &&
            book.previous_diff_update_id() == 501U &&
            book.last_update_id() == 501U,
        "post-bridge pu chain is authoritative over U range overlap");
}

void test_chain_precedes_stale_classification(Context& context) {
    SpotPayloadParser parser;
    UsdMBookState book;
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            90U,
            100U,
            89U,
            R"([["100","1"]])",
            R"([["101","1"]])"),
        "precedence seed refresh"));
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(100U, 102U, 50U, "[]", "[]"),
        "precedence bridge"));

    const SpotBookApplyResult redelivery = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(100U, 102U, 50U, R"([["100","9"]])", "[]"),
        "post-bridge stale redelivery");
    context.expect(
        redelivery.status == SpotBookApplyStatus::sequence_gap &&
            !redelivery.emits_snapshot() && !book.valid(),
        "mismatched pu invalidates before stale classification");
    context.expect(
        book.last_update_id() == 102U &&
            book.bids()[0U].quantity == 100'000'000LL,
        "re-delivered diff does not partially mutate state");
}

void test_refresh_reset_and_recovery(Context& context) {
    SpotPayloadParser parser;
    UsdMBookState book;
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            190U,
            200U,
            189U,
            R"([["100","1"]])",
            R"([["101","1"]])"),
        "reset seed refresh"));
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(200U, 202U, 1U, "[]", "[]"),
        "reset bridge"));

    const SpotBookApplyResult stale_refresh = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            190U,
            201U,
            189U,
            R"([["90","2"]])",
            R"([["91","2"]])"),
        "stale USD-M refresh");
    context.expect(
        stale_refresh.status == SpotBookApplyStatus::stale_refresh &&
            book.diff_chain_established() &&
            book.previous_diff_update_id() == 202U,
        "stale refresh does not reset an established chain");

    const SpotBookApplyResult equal_refresh = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            200U,
            202U,
            199U,
            R"([["100","4"]])",
            R"([["101","4"]])"),
        "equal-ID USD-M refresh");
    context.expect(
        equal_refresh.status == SpotBookApplyStatus::applied_refresh &&
            !book.diff_chain_established(),
        "accepted refresh resets USD-M bridge state");

    const SpotBookApplyResult gap = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(204U, 204U, 1U, "[]", "[]"),
        "pre-bridge gap");
    context.expect(
        gap.status == SpotBookApplyStatus::sequence_gap &&
            !book.valid() && !book.diff_chain_established(),
        "uncovered pre-bridge next ID invalidates");

    const SpotBookApplyResult recovered = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            40U,
            50U,
            39U,
            R"([["80","2"]])",
            R"([["81","2"]])"),
        "lower USD-M recovery refresh");
    context.expect(
        recovered.status == SpotBookApplyStatus::applied_refresh &&
            book.valid() && book.last_update_id() == 50U &&
            !book.diff_chain_established(),
        "invalid USD-M state recovers from a lower refresh");
}

void test_usdm_crossing_is_atomic(Context& context) {
    SpotPayloadParser parser;
    UsdMBookState book;
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        usdm_depth(
            90U,
            100U,
            89U,
            R"([["100","1"]])",
            R"([["101","1"]])"),
        "crossing seed refresh"));
    static_cast<void>(parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(100U, 102U, 1U, "[]", "[]"),
        "crossing bridge"));

    const SpotBookApplyResult crossed = parse_and_apply_usdm(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        usdm_depth(
            103U,
            103U,
            102U,
            R"([["102","1"]])",
            "[]"),
        "crossing chained diff");
    context.expect(
        crossed.status == SpotBookApplyStatus::crossed_book &&
            !book.valid() && !book.diff_chain_established(),
        "crossed USD-M candidate invalidates and clears chain");
    context.expect(
        book.last_update_id() == 102U &&
            book.bids()[0U].price == 10'000'000'000LL,
        "crossed USD-M candidate does not commit");
}

}  // namespace

void run_usdm_book_tests(Context& context) {
    test_usdm_bridge_and_chain(context);
    test_chain_precedes_stale_classification(context);
    test_refresh_reset_and_recovery(context);
    test_usdm_crossing_is_atomic(context);
}

}  // namespace hft::test
