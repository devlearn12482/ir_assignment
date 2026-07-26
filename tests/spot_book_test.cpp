#include "test_framework.h"

#include "hft/spot_book.h"

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace hft::test {
namespace {

class PaddedBookJson {
public:
    explicit PaddedBookJson(const std::string_view json)
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

[[nodiscard]] std::int64_t scaled(const std::int64_t value) noexcept {
    return value * 100'000'000LL;
}

[[nodiscard]] std::string refresh(
    const std::uint64_t update_id,
    const std::string_view bids =
        R"([["100","1"],["99","1"],["98","1"],["97","1"],["96","1"]])",
    const std::string_view asks =
        R"([["101","1"],["102","1"],["103","1"],["104","1"],["105","1"]])") {
    return R"({"lastUpdateId":)" + std::to_string(update_id) +
           R"(,"bids":)" + std::string{bids} +
           R"(,"asks":)" + std::string{asks} + "}";
}

[[nodiscard]] std::string diff(
    const std::uint64_t first_update_id,
    const std::uint64_t final_update_id,
    const std::string_view bids,
    const std::string_view asks) {
    return R"({"e":"depthUpdate","E":1,"s":"BTCUSDT","U":)" +
           std::to_string(first_update_id) + R"(,"u":)" +
           std::to_string(final_update_id) + R"(,"b":)" +
           std::string{bids} + R"(,"a":)" + std::string{asks} + "}";
}

[[nodiscard]] SpotBookApplyResult parse_and_apply(
    Context& context,
    SpotPayloadParser& parser,
    SpotBookState& book,
    const SpotStreamKind kind,
    const std::string& json,
    const std::string_view description) {
    const PaddedBookJson padded{json};
    const SpotParseResult parsed =
        parser.parse(kind, "BTCUSDT", padded.view());
    context.expect(
        parsed.has_value(),
        std::string{description} + " parses before book application");
    if (!parsed) {
        return {};
    }
    return kind == SpotStreamKind::depth5
               ? book.apply_refresh(parsed.event.depth)
               : book.apply_diff(parsed.event.depth);
}

void expect_level(
    Context& context,
    const BookSideView side,
    const std::size_t index,
    const std::int64_t price,
    const std::int64_t quantity,
    const std::string_view description) {
    context.expect(
        index < side.size,
        std::string{description} + " level exists");
    if (index >= side.size) {
        return;
    }
    context.expect(
        side[index].price == scaled(price) &&
            side[index].quantity == scaled(quantity),
        std::string{description} + " has expected price and quantity");
}

void expect_same_side(
    Context& context,
    const BookSideView lhs,
    const BookSideView rhs,
    const std::string_view description) {
    context.expect(
        lhs.size == rhs.size,
        std::string{description} + " has equal depth");
    if (lhs.size != rhs.size) {
        return;
    }
    for (std::size_t index = 0; index < lhs.size; ++index) {
        context.expect(
            lhs[index].price == rhs[index].price &&
                lhs[index].quantity == rhs[index].quantity,
            std::string{description} + " has equal ordered levels");
    }
}

void test_refresh_and_staleness(Context& context) {
    SpotPayloadParser parser;
    SpotBookState book;

    context.expect(!book.valid(), "new Spot book starts invalid");
    context.expect(
        book.bids().size == 0U && book.asks().size == 0U,
        "new Spot book starts empty");

    const SpotBookApplyResult initialized = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(100U),
        "initial refresh");
    context.expect(
        initialized.status == SpotBookApplyStatus::applied_refresh &&
            initialized.side == BookRowSide::neutral &&
            initialized.emits_snapshot(),
        "initial refresh emits a neutral snapshot");
    context.expect(
        book.valid() && book.last_update_id() == 100U,
        "initial refresh establishes validity and update ID");
    context.expect(
        book.bids().size == 5U && book.asks().size == 5U,
        "initial refresh replaces both five-level sides");
    expect_level(context, book.bids(), 0U, 100, 1, "best bid");
    expect_level(context, book.bids(), 4U, 96, 1, "fifth bid");
    expect_level(context, book.asks(), 0U, 101, 1, "best ask");

    const SpotBookApplyResult stale = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(99U, R"([["90","2"]])", R"([["110","2"]])"),
        "lower refresh");
    context.expect(
        stale.status == SpotBookApplyStatus::stale_refresh &&
            !stale.emits_snapshot(),
        "lower refresh is stale");
    context.expect(
        book.last_update_id() == 100U && book.bids().size == 5U,
        "stale refresh leaves state unchanged");

    const SpotBookApplyResult equal = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(100U, R"([["100","3"]])", R"([["101","4"]])"),
        "equal-ID refresh");
    context.expect(
        equal.status == SpotBookApplyStatus::applied_refresh,
        "equal-ID refresh is accepted because only lower IDs are stale");
    expect_level(context, book.bids(), 0U, 100, 3, "equal refresh bid");
    expect_level(context, book.asks(), 0U, 101, 4, "equal refresh ask");
}

void test_diff_mutation_and_side_codes(Context& context) {
    SpotPayloadParser parser;
    SpotBookState book;
    static_cast<void>(parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(100U),
        "mutation seed refresh"));

    const SpotBookApplyResult bids_only = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(
            101U,
            101U,
            R"([["100","7"],["99","0"],["95.5","2"],["500","0"]])",
            "[]"),
        "bid-only differential");
    context.expect(
        bids_only.status == SpotBookApplyStatus::applied_diff &&
            bids_only.side == BookRowSide::bid &&
            bids_only.emits_snapshot(),
        "bid-only diff applies and emits B-side metadata");
    context.expect(
        book.last_update_id() == 101U && book.bids().size == 5U,
        "applied diff advances update ID and retains bounded depth");
    expect_level(context, book.bids(), 0U, 100, 7, "replaced bid");
    expect_level(context, book.bids(), 1U, 98, 1, "post-removal bid");
    context.expect(
        book.bids()[4U].price == 9'550'000'000LL,
        "removal opens capacity for a new worse visible bid");

    const SpotBookApplyResult both = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(
            102U,
            102U,
            R"([["101","1"]])",
            R"([["101","0"],["102","3"]])"),
        "two-sided differential");
    context.expect(
        both.status == SpotBookApplyStatus::applied_diff &&
            both.side == BookRowSide::neutral,
        "two-sided diff uses neutral side metadata");
    expect_level(context, book.bids(), 0U, 101, 1, "new best bid");
    expect_level(context, book.asks(), 0U, 102, 3, "replaced best ask");

    const SpotBookApplyResult empty = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(103U, 103U, "[]", "[]"),
        "empty differential");
    context.expect(
        empty.status == SpotBookApplyStatus::applied_diff &&
            empty.side == BookRowSide::neutral &&
            book.last_update_id() == 103U,
        "accepted empty diff still advances sequence and emits a row");

    const SpotBookApplyResult asks_only = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(104U, 104U, "[]", R"([["102","9"]])"),
        "ask-only differential");
    context.expect(
        asks_only.status == SpotBookApplyStatus::applied_diff &&
            asks_only.side == BookRowSide::ask,
        "ask-only diff applies and emits S-side metadata");
    expect_level(context, book.asks(), 0U, 102, 9, "ask-only replacement");

    const BookLevel fifth_bid_before = book.bids()[4U];
    const SpotBookApplyResult outside_visible_depth = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(105U, 105U, R"([["1","1"]])", "[]"),
        "outside-depth differential");
    context.expect(
        outside_visible_depth.status ==
                SpotBookApplyStatus::applied_diff &&
            outside_visible_depth.emits_snapshot() &&
            book.last_update_id() == 105U &&
            book.bids()[4U].price == fifth_bid_before.price,
        "accepted off-book update advances sequence without changing top five");
}

void test_update_order_independence(Context& context) {
    SpotPayloadParser parser_a;
    SpotPayloadParser parser_b;
    SpotBookState book_a;
    SpotBookState book_b;
    static_cast<void>(parse_and_apply(
        context,
        parser_a,
        book_a,
        SpotStreamKind::depth5,
        refresh(
            200U,
            R"([["100","1"],["99","1"],["98","1"],["97","1"],["96","1"]])",
            R"([["201","1"],["202","1"],["203","1"],["204","1"],["205","1"]])"),
        "order-A seed refresh"));
    static_cast<void>(parse_and_apply(
        context,
        parser_b,
        book_b,
        SpotStreamKind::depth5,
        refresh(
            200U,
            R"([["100","1"],["99","1"],["98","1"],["97","1"],["96","1"]])",
            R"([["201","1"],["202","1"],["203","1"],["204","1"],["205","1"]])"),
        "order-B seed refresh"));

    const SpotBookApplyResult forward = parse_and_apply(
        context,
        parser_a,
        book_a,
        SpotStreamKind::depth_diff,
        diff(
            201U,
            201U,
            R"([["95","1"],["101","1"],["100","0"],["102","1"]])",
            "[]"),
        "forward-ordered candidate diff");
    const SpotBookApplyResult reverse = parse_and_apply(
        context,
        parser_b,
        book_b,
        SpotStreamKind::depth_diff,
        diff(
            201U,
            201U,
            R"([["102","1"],["100","0"],["101","1"],["95","1"]])",
            "[]"),
        "reverse-ordered candidate diff");
    context.expect(
        forward.status == SpotBookApplyStatus::applied_diff &&
            reverse.status == SpotBookApplyStatus::applied_diff,
        "both update permutations remain inside the spread and apply");

    expect_same_side(
        context,
        book_a.bids(),
        book_b.bids(),
        "candidate insertion order");
    expect_level(context, book_a.bids(), 0U, 102, 1, "ordered best bid");
    expect_level(context, book_a.bids(), 1U, 101, 1, "ordered second bid");
    expect_level(context, book_a.bids(), 4U, 97, 1, "ordered fifth bid");
}

void test_spot_sequence_state(Context& context) {
    SpotPayloadParser parser;
    SpotBookState book;
    static_cast<void>(parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(300U),
        "sequence seed refresh"));

    const SpotBookApplyResult stale = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(250U, 300U, R"([["110","1"]])", "[]"),
        "fully stale diff");
    context.expect(
        stale.status == SpotBookApplyStatus::stale_diff &&
            !stale.emits_snapshot() && book.last_update_id() == 300U,
        "u <= L is stale without mutation");

    const SpotBookApplyResult overlap = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(299U, 302U, R"([["100","2"]])", "[]"),
        "overlapping diff");
    context.expect(
        overlap.status == SpotBookApplyStatus::applied_diff &&
            book.last_update_id() == 302U,
        "range spanning L+1 is accepted");

    const BookSideView before_gap = book.bids();
    const BookLevel best_before_gap = before_gap[0U];
    const SpotBookApplyResult gap = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(304U, 304U, R"([["120","1"]])", "[]"),
        "gapped diff");
    context.expect(
        gap.status == SpotBookApplyStatus::sequence_gap &&
            !gap.emits_snapshot() && !book.valid(),
        "U greater than L+1 invalidates on a gap");
    context.expect(
        book.last_update_id() == 302U &&
            book.bids()[0U].price == best_before_gap.price,
        "gap does not partially mutate levels or update ID");

    const SpotBookApplyResult ignored = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(303U, 303U, R"([["130","1"]])", "[]"),
        "diff while invalid");
    context.expect(
        ignored.status == SpotBookApplyStatus::ignored_while_invalid &&
            book.last_update_id() == 302U,
        "diffs remain ignored until a refresh recovers state");

    const SpotBookApplyResult recovered = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(10U, R"([["90","2"]])", R"([["91","2"]])"),
        "lower recovery refresh");
    context.expect(
        recovered.status == SpotBookApplyStatus::applied_refresh &&
            book.valid() && book.last_update_id() == 10U,
        "invalid state accepts a lower refresh and re-establishes sequence");
}

void test_crossed_candidate_is_atomic(Context& context) {
    SpotPayloadParser parser;
    SpotBookState book;
    static_cast<void>(parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(400U, R"([["100","1"]])", R"([["101","1"]])"),
        "cross seed refresh"));

    const BookLevel original_ask = book.asks()[0U];
    const SpotBookApplyResult crossed = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(
            401U,
            401U,
            "[]",
            R"([["101","0"],["99","2"]])"),
        "crossing differential");
    context.expect(
        crossed.status == SpotBookApplyStatus::crossed_book &&
            !crossed.emits_snapshot() && !book.valid(),
        "crossed candidate invalidates without a snapshot");
    context.expect(
        book.last_update_id() == 400U && book.asks().size == 1U &&
            book.asks()[0U].price == original_ask.price,
        "crossed candidate leaves active levels and update ID uncommitted");
}

void test_boundaries_and_explicit_invalidation(Context& context) {
    SpotPayloadParser parser;
    SpotBookState book;
    const std::uint64_t maximum =
        std::numeric_limits<std::uint64_t>::max();
    static_cast<void>(parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth5,
        refresh(maximum, R"([["100","1"]])", R"([["101","1"]])"),
        "maximum-ID refresh"));

    const SpotBookApplyResult maximum_stale = parse_and_apply(
        context,
        parser,
        book,
        SpotStreamKind::depth_diff,
        diff(maximum, maximum, "[]", "[]"),
        "maximum-ID stale diff");
    context.expect(
        maximum_stale.status == SpotBookApplyStatus::stale_diff &&
            book.valid() && book.last_update_id() == maximum,
        "maximum last ID classifies stale without overflowing L+1");

    book.invalidate();
    context.expect(
        !book.valid() && book.last_update_id() == maximum &&
            book.bids().size == 1U,
        "explicit epoch invalidation preserves diagnostic state but blocks diffs");
}

}  // namespace

void run_spot_book_tests(Context& context) {
    test_refresh_and_staleness(context);
    test_diff_mutation_and_side_codes(context);
    test_update_order_independence(context);
    test_spot_sequence_state(context);
    test_crossed_candidate_is_atomic(context);
    test_boundaries_and_explicit_invalidation(context);
}

}  // namespace hft::test
