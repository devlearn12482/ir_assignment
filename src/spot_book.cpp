#include "hft/spot_book.h"

#include <cassert>
#include <cstddef>

namespace hft {
namespace {

static_assert(kVisibleBookDepth == kMaxPartialDepthLevelsPerSide);

[[nodiscard]] bool better_price(
    const BookSide side,
    const std::int64_t lhs,
    const std::int64_t rhs) noexcept {
    return side == BookSide::bid ? lhs > rhs : lhs < rhs;
}

[[nodiscard]] BookRowSide row_side(
    const SpotDepthEvent& event) noexcept {
    const bool has_bids = event.bids.size != 0U;
    const bool has_asks = event.asks.size != 0U;
    if (has_bids == has_asks) {
        return BookRowSide::neutral;
    }
    return has_bids ? BookRowSide::bid : BookRowSide::ask;
}

}  // namespace

bool SpotBookState::valid() const noexcept {
    return valid_;
}

std::uint64_t SpotBookState::last_update_id() const noexcept {
    return last_update_id_;
}

BookSideView SpotBookState::bids() const noexcept {
    return BookSideView{bids_.levels.data(), bids_.size};
}

BookSideView SpotBookState::asks() const noexcept {
    return BookSideView{asks_.levels.data(), asks_.size};
}

void SpotBookState::invalidate() noexcept {
    valid_ = false;
}

std::size_t SpotBookState::find_price(
    const Side& side,
    const std::int64_t price) noexcept {
    for (std::size_t index = 0; index < side.size; ++index) {
        if (side.levels[index].price == price) {
            return index;
        }
    }
    return side.size;
}

void SpotBookState::erase_level(
    Side& side,
    const std::size_t index) noexcept {
    assert(index < side.size);
    for (std::size_t destination = index;
         destination + 1U < side.size;
         ++destination) {
        side.levels[destination] = side.levels[destination + 1U];
    }
    --side.size;
    side.levels[side.size] = {};
}

void SpotBookState::insert_level(
    Side& side,
    const BookSide book_side,
    const BookLevel level) noexcept {
    std::size_t position{0};
    while (position < side.size &&
           better_price(
               book_side, side.levels[position].price, level.price)) {
        ++position;
    }

    if (side.size == kVisibleBookDepth &&
        position == kVisibleBookDepth) {
        return;
    }

    const std::size_t destination_limit =
        side.size < kVisibleBookDepth ? side.size
                                     : kVisibleBookDepth - 1U;
    for (std::size_t destination = destination_limit;
         destination > position;
         --destination) {
        side.levels[destination] = side.levels[destination - 1U];
    }
    side.levels[position] = level;
    if (side.size < kVisibleBookDepth) {
        ++side.size;
    }
}

SpotBookState::Side SpotBookState::make_refresh_side(
    const LevelUpdateRange updates,
    const BookSide side) noexcept {
    static_cast<void>(side);
    assert(updates.size <= kVisibleBookDepth);
    assert(updates.size == 0U || updates.data != nullptr);
    Side result;
    for (const LevelUpdate& update : updates) {
        assert(update.side == side);
        assert(update.price > 0);
        assert(update.quantity > 0);
        result.levels[result.size] =
            BookLevel{update.price, update.quantity};
        ++result.size;
    }
    return result;
}

SpotBookState::Side SpotBookState::apply_updates(
    const Side& original,
    const LevelUpdateRange updates,
    const BookSide side) noexcept {
    assert(updates.size == 0U || updates.data != nullptr);
    Side candidate = original;

    for (const LevelUpdate& update : updates) {
        assert(update.side == side);
        assert(update.price > 0);
        assert(update.quantity >= 0);
        const std::size_t original_index =
            find_price(original, update.price);
        if (original_index == original.size) {
            continue;
        }

        const std::size_t candidate_index =
            find_price(candidate, update.price);
        assert(candidate_index < candidate.size);
        if (update.quantity == 0) {
            erase_level(candidate, candidate_index);
        } else {
            candidate.levels[candidate_index].quantity =
                update.quantity;
        }
    }

    for (const LevelUpdate& update : updates) {
        if (update.quantity == 0 ||
            find_price(original, update.price) != original.size) {
            continue;
        }
        insert_level(
            candidate,
            side,
            BookLevel{update.price, update.quantity});
    }

    return candidate;
}

bool SpotBookState::crossed(
    const Side& bids,
    const Side& asks) noexcept {
    return bids.size != 0U && asks.size != 0U &&
           bids.levels[0U].price >= asks.levels[0U].price;
}

SpotBookApplyResult SpotBookState::apply_refresh(
    const SpotDepthEvent& event) noexcept {
    const std::uint64_t refresh_id = event.final_update_id;
    if (valid_ && refresh_id < last_update_id_) {
        return SpotBookApplyResult{
            SpotBookApplyStatus::stale_refresh,
            BookRowSide::neutral};
    }

    const Side candidate_bids =
        make_refresh_side(event.bids, BookSide::bid);
    const Side candidate_asks =
        make_refresh_side(event.asks, BookSide::ask);
    assert(!crossed(candidate_bids, candidate_asks));

    bids_ = candidate_bids;
    asks_ = candidate_asks;
    last_update_id_ = refresh_id;
    valid_ = true;
    return SpotBookApplyResult{
        SpotBookApplyStatus::applied_refresh,
        BookRowSide::neutral};
}

SpotBookApplyResult SpotBookState::apply_diff(
    const SpotDepthEvent& event) noexcept {
    const BookRowSide side = row_side(event);
    if (!valid_) {
        return SpotBookApplyResult{
            SpotBookApplyStatus::ignored_while_invalid, side};
    }

    if (event.final_update_id <= last_update_id_) {
        return SpotBookApplyResult{
            SpotBookApplyStatus::stale_diff, side};
    }

    const std::uint64_t next_update_id = last_update_id_ + 1U;
    if (event.first_update_id > next_update_id) {
        invalidate();
        return SpotBookApplyResult{
            SpotBookApplyStatus::sequence_gap, side};
    }

    const Side candidate_bids =
        apply_updates(bids_, event.bids, BookSide::bid);
    const Side candidate_asks =
        apply_updates(asks_, event.asks, BookSide::ask);
    if (crossed(candidate_bids, candidate_asks)) {
        invalidate();
        return SpotBookApplyResult{
            SpotBookApplyStatus::crossed_book, side};
    }

    bids_ = candidate_bids;
    asks_ = candidate_asks;
    last_update_id_ = event.final_update_id;
    return SpotBookApplyResult{
        SpotBookApplyStatus::applied_diff, side};
}

}  // namespace hft
