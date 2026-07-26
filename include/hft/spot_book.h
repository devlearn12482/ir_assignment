#pragma once

#include "hft/spot_payload_parser.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hft {

inline constexpr std::size_t kVisibleBookDepth{5U};

struct BookLevel {
    std::int64_t price{0};
    std::int64_t quantity{0};
};

struct BookSideView {
    const BookLevel* data{nullptr};
    std::size_t size{0};

    [[nodiscard]] const BookLevel* begin() const noexcept {
        return data;
    }

    [[nodiscard]] const BookLevel* end() const noexcept {
        return data == nullptr ? nullptr : data + size;
    }

    [[nodiscard]] const BookLevel& operator[](
        const std::size_t index) const noexcept {
        return data[index];
    }
};

enum class BookRowSide : std::uint8_t {
    bid,
    ask,
    neutral,
};

enum class SpotBookApplyStatus : std::uint8_t {
    applied_refresh,
    applied_diff,
    stale_refresh,
    stale_diff,
    ignored_while_invalid,
    sequence_gap,
    crossed_book,
};

struct SpotBookApplyResult {
    SpotBookApplyStatus status{SpotBookApplyStatus::ignored_while_invalid};
    BookRowSide side{BookRowSide::neutral};

    [[nodiscard]] bool emits_snapshot() const noexcept {
        return status == SpotBookApplyStatus::applied_refresh ||
               status == SpotBookApplyStatus::applied_diff;
    }
};

class SpotBookState {
public:
    SpotBookState() = default;

    [[nodiscard]] bool valid() const noexcept;
    [[nodiscard]] std::uint64_t last_update_id() const noexcept;
    [[nodiscard]] BookSideView bids() const noexcept;
    [[nodiscard]] BookSideView asks() const noexcept;

    void invalidate() noexcept;

    // Events must come from a successful synchronous SpotPayloadParser call.
    // Its LevelUpdateRange views are consumed before that parser is reused.
    [[nodiscard]] SpotBookApplyResult apply_refresh(
        const SpotDepthEvent& event) noexcept;
    [[nodiscard]] SpotBookApplyResult apply_diff(
        const SpotDepthEvent& event) noexcept;

private:
    struct Side {
        std::array<BookLevel, kVisibleBookDepth> levels{};
        std::size_t size{0};
    };

    [[nodiscard]] static std::size_t find_price(
        const Side& side,
        std::int64_t price) noexcept;
    static void erase_level(
        Side& side,
        std::size_t index) noexcept;
    static void insert_level(
        Side& side,
        BookSide book_side,
        BookLevel level) noexcept;
    [[nodiscard]] static Side make_refresh_side(
        LevelUpdateRange updates,
        BookSide side) noexcept;
    [[nodiscard]] static Side apply_updates(
        const Side& original,
        LevelUpdateRange updates,
        BookSide side) noexcept;
    [[nodiscard]] static bool crossed(
        const Side& bids,
        const Side& asks) noexcept;

    Side bids_{};
    Side asks_{};
    std::uint64_t last_update_id_{0};
    bool valid_{false};
};

[[nodiscard]] constexpr std::string_view to_string(
    const SpotBookApplyStatus status) noexcept {
    switch (status) {
        case SpotBookApplyStatus::applied_refresh:
            return "applied_refresh";
        case SpotBookApplyStatus::applied_diff:
            return "applied_diff";
        case SpotBookApplyStatus::stale_refresh:
            return "stale_refresh";
        case SpotBookApplyStatus::stale_diff:
            return "stale_diff";
        case SpotBookApplyStatus::ignored_while_invalid:
            return "ignored_while_invalid";
        case SpotBookApplyStatus::sequence_gap:
            return "sequence_gap";
        case SpotBookApplyStatus::crossed_book:
            return "crossed_book";
    }
    return "unknown";
}

}  // namespace hft
