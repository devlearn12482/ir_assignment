#pragma once

#include "hft/csv_formatter.h"
#include "hft/symbol_identity.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace hft {

enum class EventOutputMode : std::uint8_t {
    live_capture,
    replay,
};

enum class EventProcessStatus : std::uint8_t {
    not_processed,
    applied_refresh,
    applied_diff,
    stale_refresh,
    stale_diff,
    ignored_while_invalid,
    sequence_gap,
    crossed_book,
    trade_audited,
    schema_rejected,
    pre_audit_rejected,
};

enum class EventProcessError : std::uint8_t {
    none,
    invalid_context,
    sequence_overflow,
    csv_format_error,
};

struct EventContext {
    CsvTimestamp timestamp{};
    PayloadVenue venue{PayloadVenue::spot};
    SpotStreamKind stream_kind{SpotStreamKind::depth_diff};
    // Baseline topology is one connection and shard_id == 0.
    std::uint32_t shard_id{0};
    std::uint64_t connection_epoch{0};
    std::uint64_t connection_sequence{0};
    std::string_view symbol{};
};

class EventRowBatch {
public:
    EventRowBatch();

    void clear() noexcept;

    CsvRecordBuffer audit_row;
    CsvRecordBuffer order_book_row;
    bool has_audit_row{false};
    bool has_order_book_row{false};
};

struct EventProcessResult {
    EventProcessStatus status{EventProcessStatus::not_processed};
    EventProcessError error{EventProcessError::none};
    SpotParseError parse_error{SpotParseError::none};
    SpotField parse_field{SpotField::none};
    CsvFormatError csv_error{CsvFormatError::none};
    BookApplyStatus book_status{
        BookApplyStatus::ignored_while_invalid};
    bool has_book_status{false};
    bool trade_diagnostic_mismatch{false};

    [[nodiscard]] bool success() const noexcept {
        return error == EventProcessError::none;
    }
};

class EventProcessor;

class SymbolState {
public:
    [[nodiscard]] static std::optional<SymbolState> create(
        PayloadVenue venue,
        std::string_view normalized_symbol) noexcept;

    [[nodiscard]] PayloadVenue venue() const noexcept;
    [[nodiscard]] std::string_view symbol() const noexcept;
    [[nodiscard]] std::int32_t instrument_id() const noexcept;
    [[nodiscard]] std::uint64_t sequence_number() const noexcept;
    [[nodiscard]] bool book_valid() const noexcept;
    [[nodiscard]] std::uint64_t last_update_id() const noexcept;
    [[nodiscard]] bool has_observed_connection() const noexcept;
    [[nodiscard]] std::uint64_t current_connection_epoch() const noexcept;
    [[nodiscard]] std::uint64_t last_connection_sequence() const noexcept;

    // Session teardown invalidates immediately; seqNo remains per-file and
    // therefore does not reset across reconnects.
    void invalidate() noexcept;

private:
    SymbolState(
        PayloadVenue venue,
        std::string_view normalized_symbol,
        std::int32_t instrument_id) noexcept;

    std::array<char, kMaxNormalizedSymbolBytes> symbol_bytes_{};
    std::size_t symbol_size_{0};
    PayloadVenue venue_{PayloadVenue::spot};
    std::int32_t instrument_id_{0};
    std::uint64_t sequence_number_{0};
    bool has_observed_connection_{false};
    std::uint64_t current_connection_epoch_{0};
    std::uint64_t last_connection_sequence_{0};
    SpotBookState spot_book_{};
    UsdMBookState usdm_book_{};

    friend class EventProcessor;
};

// Returns false and leaves next unchanged when current is UINT64_MAX.
[[nodiscard]] bool checked_next_sequence(
    std::uint64_t current,
    std::uint64_t& next) noexcept;

class EventProcessor {
public:
    EventProcessor() = default;

    // payload is consumed synchronously. Live callers provide the minified
    // inner data object; replay callers provide the decoded persisted field.
    [[nodiscard]] EventProcessResult process(
        SymbolState& state,
        const EventContext& context,
        PaddedJsonView payload,
        EventOutputMode output_mode,
        EventRowBatch& output) noexcept;

private:
    SpotPayloadParser parser_{};
};

[[nodiscard]] constexpr std::string_view to_string(
    const EventProcessError error) noexcept {
    switch (error) {
        case EventProcessError::none:
            return "none";
        case EventProcessError::invalid_context:
            return "invalid_context";
        case EventProcessError::sequence_overflow:
            return "sequence_overflow";
        case EventProcessError::csv_format_error:
            return "csv_format_error";
    }
    return "unknown";
}

[[nodiscard]] constexpr std::string_view to_string(
    const EventProcessStatus status) noexcept {
    switch (status) {
        case EventProcessStatus::not_processed:
            return "not_processed";
        case EventProcessStatus::applied_refresh:
            return "applied_refresh";
        case EventProcessStatus::applied_diff:
            return "applied_diff";
        case EventProcessStatus::stale_refresh:
            return "stale_refresh";
        case EventProcessStatus::stale_diff:
            return "stale_diff";
        case EventProcessStatus::ignored_while_invalid:
            return "ignored_while_invalid";
        case EventProcessStatus::sequence_gap:
            return "sequence_gap";
        case EventProcessStatus::crossed_book:
            return "crossed_book";
        case EventProcessStatus::trade_audited:
            return "trade_audited";
        case EventProcessStatus::schema_rejected:
            return "schema_rejected";
        case EventProcessStatus::pre_audit_rejected:
            return "pre_audit_rejected";
    }
    return "unknown";
}

}  // namespace hft
