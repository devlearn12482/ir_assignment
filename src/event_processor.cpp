#include "hft/event_processor.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string_view>

namespace hft {
namespace {

inline constexpr std::uint32_t kNanosecondsPerSecond{
    1'000'000'000U};
inline constexpr std::size_t kOrderBookRecordCapacity{1024U};

[[nodiscard]] bool valid_venue(
    const PayloadVenue venue) noexcept {
    switch (venue) {
        case PayloadVenue::spot:
        case PayloadVenue::usdm:
            return true;
    }
    return false;
}

[[nodiscard]] bool valid_stream_kind(
    const SpotStreamKind kind) noexcept {
    switch (kind) {
        case SpotStreamKind::depth_diff:
        case SpotStreamKind::depth5:
        case SpotStreamKind::trade:
            return true;
    }
    return false;
}

[[nodiscard]] bool valid_output_mode(
    const EventOutputMode mode) noexcept {
    switch (mode) {
        case EventOutputMode::live_capture:
        case EventOutputMode::replay:
            return true;
    }
    return false;
}

[[nodiscard]] bool audit_eligible(
    const SpotParseError error) noexcept {
    switch (error) {
        case SpotParseError::none:
        case SpotParseError::missing_field:
        case SpotParseError::duplicate_field:
        case SpotParseError::wrong_type:
        case SpotParseError::unexpected_event_type:
        case SpotParseError::symbol_mismatch:
        case SpotParseError::invalid_decimal:
        case SpotParseError::non_positive_price:
        case SpotParseError::invalid_quantity:
        case SpotParseError::invalid_update_range:
        case SpotParseError::too_many_levels:
        case SpotParseError::invalid_level_shape:
        case SpotParseError::duplicate_price:
        case SpotParseError::invalid_level_order:
        case SpotParseError::crossed_book:
            return true;
        case SpotParseError::invalid_input_buffer:
        case SpotParseError::payload_too_large:
        case SpotParseError::json_nesting_too_deep:
        case SpotParseError::malformed_json:
        case SpotParseError::root_not_object:
        case SpotParseError::internal_capacity_error:
            return false;
    }
    return false;
}

[[nodiscard]] EventProcessStatus event_status(
    const BookApplyStatus status) noexcept {
    switch (status) {
        case BookApplyStatus::applied_refresh:
            return EventProcessStatus::applied_refresh;
        case BookApplyStatus::applied_diff:
            return EventProcessStatus::applied_diff;
        case BookApplyStatus::stale_refresh:
            return EventProcessStatus::stale_refresh;
        case BookApplyStatus::stale_diff:
            return EventProcessStatus::stale_diff;
        case BookApplyStatus::ignored_while_invalid:
            return EventProcessStatus::ignored_while_invalid;
        case BookApplyStatus::sequence_gap:
            return EventProcessStatus::sequence_gap;
        case BookApplyStatus::crossed_book:
            return EventProcessStatus::crossed_book;
    }
    return EventProcessStatus::not_processed;
}

[[nodiscard]] BookSideView candidate_bids(
    const PayloadVenue venue,
    const SpotBookState& spot,
    const UsdMBookState& usdm) noexcept {
    return venue == PayloadVenue::spot ? spot.bids() : usdm.bids();
}

[[nodiscard]] BookSideView candidate_asks(
    const PayloadVenue venue,
    const SpotBookState& spot,
    const UsdMBookState& usdm) noexcept {
    return venue == PayloadVenue::spot ? spot.asks() : usdm.asks();
}

[[nodiscard]] bool context_is_valid(
    const SymbolState& state,
    const EventContext& context,
    const EventOutputMode output_mode) noexcept {
    if (!valid_output_mode(output_mode) ||
        !valid_stream_kind(context.stream_kind) ||
        context.venue != state.venue() ||
        context.symbol != state.symbol() ||
        context.shard_id != 0U ||
        context.timestamp.nanoseconds >= kNanosecondsPerSecond ||
        context.connection_sequence == 0U) {
        return false;
    }
    if (!state.has_observed_connection()) {
        return true;
    }
    if (context.connection_epoch <
        state.current_connection_epoch()) {
        return false;
    }
    return context.connection_epoch !=
               state.current_connection_epoch() ||
           context.connection_sequence >
               state.last_connection_sequence();
}

[[nodiscard]] CsvFormatError format_audit_row(
    const EventContext& context,
    const PaddedJsonView payload,
    EventRowBatch& output) noexcept {
    const MarketDataCsvRow row{
        context.timestamp,
        context.venue,
        context.stream_kind,
        context.shard_id,
        context.connection_epoch,
        context.connection_sequence,
        context.symbol,
        std::string_view{payload.data, payload.size},
    };
    const CsvFormatError error =
        format_market_data_csv_row(row, output.audit_row);
    output.has_audit_row = error == CsvFormatError::none;
    return error;
}

}  // namespace

EventRowBatch::EventRowBatch()
    : audit_row{},
      order_book_row{kOrderBookRecordCapacity} {}

void EventRowBatch::clear() noexcept {
    audit_row.clear();
    order_book_row.clear();
    has_audit_row = false;
    has_order_book_row = false;
}

bool EventRowBatch::release_excess_capacity() noexcept {
    has_audit_row = false;
    has_order_book_row = false;
    const bool audit_released =
        audit_row.release_excess_capacity();
    const bool book_released =
        order_book_row.release_excess_capacity();
    return audit_released && book_released;
}

std::optional<SymbolState> SymbolState::create(
    const PayloadVenue venue,
    const std::string_view normalized_symbol) noexcept {
    if (!valid_venue(venue)) {
        return std::nullopt;
    }
    const InstrumentIdResult id =
        derive_instrument_id(normalized_symbol);
    if (!id) {
        return std::nullopt;
    }
    return SymbolState{venue, normalized_symbol, id.value};
}

SymbolState::SymbolState(
    const PayloadVenue venue,
    const std::string_view normalized_symbol,
    const std::int32_t instrument_id) noexcept
    : symbol_size_{normalized_symbol.size()},
      venue_{venue},
      instrument_id_{instrument_id} {
    std::copy(
        normalized_symbol.begin(),
        normalized_symbol.end(),
        symbol_bytes_.begin());
}

PayloadVenue SymbolState::venue() const noexcept {
    return venue_;
}

std::string_view SymbolState::symbol() const noexcept {
    return std::string_view{symbol_bytes_.data(), symbol_size_};
}

std::int32_t SymbolState::instrument_id() const noexcept {
    return instrument_id_;
}

std::uint64_t SymbolState::sequence_number() const noexcept {
    return sequence_number_;
}

bool SymbolState::book_valid() const noexcept {
    return venue_ == PayloadVenue::spot
               ? spot_book_.valid()
               : usdm_book_.valid();
}

std::uint64_t SymbolState::last_update_id() const noexcept {
    return venue_ == PayloadVenue::spot
               ? spot_book_.last_update_id()
               : usdm_book_.last_update_id();
}

bool SymbolState::has_observed_connection() const noexcept {
    return has_observed_connection_;
}

std::uint64_t SymbolState::current_connection_epoch() const noexcept {
    return current_connection_epoch_;
}

std::uint64_t SymbolState::last_connection_sequence() const noexcept {
    return last_connection_sequence_;
}

void SymbolState::invalidate() noexcept {
    if (venue_ == PayloadVenue::spot) {
        spot_book_.invalidate();
    } else {
        usdm_book_.invalidate();
    }
}

bool checked_next_sequence(
    const std::uint64_t current,
    std::uint64_t& next) noexcept {
    if (current == std::numeric_limits<std::uint64_t>::max()) {
        return false;
    }
    next = current + 1U;
    return true;
}

EventProcessResult EventProcessor::process(
    SymbolState& state,
    const EventContext& context,
    const PaddedJsonView payload,
    const EventOutputMode output_mode,
    EventRowBatch& output) noexcept {
    output.clear();
    EventProcessResult result;
    if (!context_is_valid(state, context, output_mode)) {
        result.error = EventProcessError::invalid_context;
        return result;
    }

    const bool epoch_changed =
        state.has_observed_connection_ &&
        context.connection_epoch >
            state.current_connection_epoch_;

    const SpotParseResult parsed = parser_.parse(
        context.venue,
        context.stream_kind,
        state.symbol(),
        payload);
    result.parse_error = parsed.error;
    result.parse_field = parsed.field;

    const auto commit_context = [&]() noexcept {
        state.has_observed_connection_ = true;
        state.current_connection_epoch_ =
            context.connection_epoch;
        state.last_connection_sequence_ =
            context.connection_sequence;
    };

    if (!audit_eligible(parsed.error)) {
        if (epoch_changed ||
            context.stream_kind ==
                SpotStreamKind::depth_diff) {
            state.invalidate();
        }
        commit_context();
        result.status =
            EventProcessStatus::pre_audit_rejected;
        return result;
    }

    if (output_mode == EventOutputMode::live_capture) {
        const CsvFormatError audit_error =
            format_audit_row(context, payload, output);
        if (audit_error != CsvFormatError::none) {
            output.clear();
            result.error = EventProcessError::csv_format_error;
            result.csv_error = audit_error;
            return result;
        }
    }

    if (!parsed) {
        if (epoch_changed ||
            context.stream_kind ==
                SpotStreamKind::depth_diff) {
            state.invalidate();
        }
        commit_context();
        result.status = EventProcessStatus::schema_rejected;
        return result;
    }

    if (context.stream_kind == SpotStreamKind::trade) {
        result.trade_diagnostic_mismatch =
            !parsed.event.trade.event_type_matches ||
            !parsed.event.trade.symbol_matches;
        if (epoch_changed) {
            state.invalidate();
        }
        commit_context();
        result.status = EventProcessStatus::trade_audited;
        return result;
    }

    SpotBookState spot_candidate;
    UsdMBookState usdm_candidate;
    BookApplyResult book_result;
    if (context.venue == PayloadVenue::spot) {
        spot_candidate = state.spot_book_;
        if (epoch_changed) {
            spot_candidate.invalidate();
        }
        book_result =
            context.stream_kind == SpotStreamKind::depth5
                ? spot_candidate.apply_refresh(parsed.event.depth)
                : spot_candidate.apply_diff(parsed.event.depth);
    } else {
        usdm_candidate = state.usdm_book_;
        if (epoch_changed) {
            usdm_candidate.invalidate();
        }
        book_result =
            context.stream_kind == SpotStreamKind::depth5
                ? usdm_candidate.apply_refresh(parsed.event.depth)
                : usdm_candidate.apply_diff(parsed.event.depth);
    }
    result.book_status = book_result.status;
    result.has_book_status = true;
    result.status = event_status(book_result.status);

    std::uint64_t next_sequence = state.sequence_number_;
    if (book_result.emits_snapshot()) {
        if (!checked_next_sequence(
                state.sequence_number_, next_sequence)) {
            output.clear();
            result.error = EventProcessError::sequence_overflow;
            result.status = EventProcessStatus::not_processed;
            return result;
        }
        const OrderBookCsvRow order_book_row{
            context.timestamp,
            next_sequence,
            state.instrument_id_,
            context.stream_kind == SpotStreamKind::depth_diff
                ? BookRowType::differential
                : BookRowType::partial_refresh,
            book_result.side,
            candidate_bids(
                context.venue, spot_candidate, usdm_candidate),
            candidate_asks(
                context.venue, spot_candidate, usdm_candidate),
        };
        const CsvFormatError book_error =
            format_order_book_csv_row(
                order_book_row, output.order_book_row);
        if (book_error != CsvFormatError::none) {
            output.clear();
            result.error = EventProcessError::csv_format_error;
            result.csv_error = book_error;
            result.status = EventProcessStatus::not_processed;
            return result;
        }
        output.has_order_book_row = true;
    }

    if (context.venue == PayloadVenue::spot) {
        state.spot_book_ = spot_candidate;
    } else {
        state.usdm_book_ = usdm_candidate;
    }
    commit_context();
    if (book_result.emits_snapshot()) {
        state.sequence_number_ = next_sequence;
    }
    return result;
}

}  // namespace hft
