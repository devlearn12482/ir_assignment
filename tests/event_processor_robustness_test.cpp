#include "test_framework.h"

#include "hft/event_processor.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hft {

// Narrow fault-injection seam: production exposes no mutable counter or
// identity setter, while tests can drive otherwise unreachable fatal paths.
struct EventProcessorTestAccess {
    static void set_sequence_number(
        SymbolState& state,
        const std::uint64_t value) noexcept {
        state.sequence_number_ = value;
    }

    static void set_instrument_id(
        SymbolState& state,
        const std::int32_t value) noexcept {
        state.instrument_id_ = value;
    }
};

}  // namespace hft

namespace hft::test {
namespace {

class PaddedRobustnessJson {
public:
    explicit PaddedRobustnessJson(const std::string_view json)
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

[[nodiscard]] EventContext robust_context(
    const PayloadVenue venue,
    const SpotStreamKind kind,
    const std::uint64_t connection_sequence,
    const std::uint64_t epoch = 0U,
    const std::string_view symbol = "BTCUSDT") noexcept {
    return EventContext{
        CsvTimestamp{1'700'000'123, 987'654'321},
        venue,
        kind,
        0U,
        epoch,
        connection_sequence,
        symbol,
    };
}

[[nodiscard]] EventProcessResult robust_process(
    EventProcessor& processor,
    SymbolState& state,
    EventRowBatch& batch,
    const EventContext& context,
    const std::string_view json,
    const EventOutputMode mode = EventOutputMode::live_capture) {
    const PaddedRobustnessJson padded{json};
    return processor.process(
        state, context, padded.view(), mode, batch);
}

[[nodiscard]] std::optional<SymbolState> robust_state(
    Context& context,
    const PayloadVenue venue) {
    std::optional<SymbolState> state =
        SymbolState::create(venue, "BTCUSDT");
    context.expect(
        state.has_value(),
        "robustness fixture creates valid symbol state");
    return state;
}

[[nodiscard]] std::size_t comma_count(
    const std::string_view row) noexcept {
    std::size_t count{0};
    for (const char byte : row) {
        if (byte == ',') {
            ++count;
        }
    }
    return count;
}

void test_fatal_paths_are_atomic(Context& context) {
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";

    std::optional<SymbolState> overflow_state =
        robust_state(context, PayloadVenue::spot);
    if (!overflow_state) {
        return;
    }
    EventProcessor overflow_processor;
    EventRowBatch overflow_batch;
    EventProcessorTestAccess::set_sequence_number(
        *overflow_state,
        std::numeric_limits<std::uint64_t>::max());
    const EventProcessResult overflow = robust_process(
        overflow_processor,
        *overflow_state,
        overflow_batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 1U),
        refresh);
    context.expect(
        overflow.error == EventProcessError::sequence_overflow &&
            overflow.status == EventProcessStatus::not_processed,
        "integrated seqNo overflow is reported before commit");
    context.expect(
        !overflow_batch.has_audit_row &&
            !overflow_batch.has_order_book_row &&
            overflow_batch.audit_row.empty() &&
            overflow_batch.order_book_row.empty(),
        "seqNo overflow exposes no half-batch");
    context.expect(
        !overflow_state->book_valid() &&
            !overflow_state->has_observed_connection() &&
            overflow_state->sequence_number() ==
                std::numeric_limits<std::uint64_t>::max(),
        "seqNo overflow leaves book and connection state unchanged");

    std::optional<SymbolState> format_state =
        robust_state(context, PayloadVenue::spot);
    if (!format_state) {
        return;
    }
    EventProcessor format_processor;
    EventRowBatch format_batch;
    EventProcessorTestAccess::set_instrument_id(*format_state, 0);
    const EventProcessResult first_format_failure = robust_process(
        format_processor,
        *format_state,
        format_batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 1U),
        refresh);
    context.expect(
        first_format_failure.error ==
                EventProcessError::csv_format_error &&
            first_format_failure.csv_error ==
                CsvFormatError::invalid_instrument_id,
        "fault-injected formatter rejection is surfaced precisely");
    context.expect(
        !format_batch.has_audit_row &&
            !format_batch.has_order_book_row &&
            !format_state->book_valid() &&
            !format_state->has_observed_connection() &&
            format_state->sequence_number() == 0U,
        "formatter rejection clears audit row and commits no state");

    std::optional<SymbolState> established =
        robust_state(context, PayloadVenue::spot);
    if (!established) {
        return;
    }
    EventProcessor established_processor;
    EventRowBatch established_batch;
    static_cast<void>(robust_process(
        established_processor,
        *established,
        established_batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 1U),
        refresh));
    EventProcessorTestAccess::set_instrument_id(*established, 0);
    constexpr std::string_view diff =
        R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100","2"]],"a":[]})";
    const EventProcessResult later_format_failure = robust_process(
        established_processor,
        *established,
        established_batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 2U),
        diff);
    context.expect(
        later_format_failure.error ==
                EventProcessError::csv_format_error &&
            !established_batch.has_audit_row &&
            !established_batch.has_order_book_row,
        "later formatter failure also exposes no half-batch");
    context.expect(
        established->book_valid() &&
            established->last_update_id() == 100U &&
            established->sequence_number() == 1U &&
            established->last_connection_sequence() == 1U,
        "later formatter failure preserves previously committed state");
}

void test_pre_audit_policy_matrix(Context& context) {
    std::optional<SymbolState> state =
        robust_state(context, PayloadVenue::spot);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";
    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 1U),
        refresh));

    const EventProcessResult invalid_buffer = processor.process(
        *state,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 2U),
        PaddedJsonView{nullptr, 0U, 0U},
        EventOutputMode::live_capture,
        batch);
    context.expect(
        invalid_buffer.status ==
                EventProcessStatus::pre_audit_rejected &&
            invalid_buffer.parse_error ==
                SpotParseError::invalid_input_buffer &&
            !batch.has_audit_row && !state->book_valid(),
        "invalid differential input buffer emits no row and invalidates");

    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 3U),
        refresh));
    const EventProcessResult non_object = robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 4U),
        R"([1,2,3])");
    context.expect(
        non_object.status ==
                EventProcessStatus::pre_audit_rejected &&
            non_object.parse_error ==
                SpotParseError::root_not_object &&
            !batch.has_audit_row && state->book_valid() &&
            state->last_update_id() == 100U,
        "non-object independent refresh preserves prior book");

    std::string nested = R"({"future":)";
    nested.append(kMaxJsonNestingDepth, '[');
    nested.push_back('0');
    nested.append(kMaxJsonNestingDepth, ']');
    nested.push_back('}');
    const EventProcessResult excessive_depth = robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth_diff, 5U),
        nested);
    context.expect(
        excessive_depth.status ==
                EventProcessStatus::pre_audit_rejected &&
            excessive_depth.parse_error ==
                SpotParseError::json_nesting_too_deep &&
            !batch.has_audit_row && !state->book_valid(),
        "nesting-policy rejection invalidates differential state");

    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 6U),
        refresh));
    std::vector<char> oversized(
        kMaxPayloadBytes + 1U + kJsonPaddingBytes, 'x');
    const EventProcessResult oversized_refresh = processor.process(
        *state,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 7U),
        PaddedJsonView{
            oversized.data(),
            kMaxPayloadBytes + 1U,
            oversized.size()},
        EventOutputMode::live_capture,
        batch);
    context.expect(
        oversized_refresh.status ==
                EventProcessStatus::pre_audit_rejected &&
            oversized_refresh.parse_error ==
                SpotParseError::payload_too_large &&
            !batch.has_audit_row && state->book_valid(),
        "oversized independent refresh emits no row and preserves book");
}

void test_long_spot_chain(Context& context) {
    std::optional<SymbolState> state =
        robust_state(context, PayloadVenue::spot);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})";
    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::spot, SpotStreamKind::depth5, 1U),
        refresh));

    bool all_applied = true;
    bool all_rows_well_formed = true;
    constexpr std::uint64_t event_count{1000U};
    for (std::uint64_t index = 0; index < event_count; ++index) {
        const std::uint64_t update_id = 101U + index;
        const std::string payload =
            R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":)" +
            std::to_string(update_id) + R"(,"u":)" +
            std::to_string(update_id) +
            R"(,"b":[["100",")" +
            std::to_string(index + 2U) + R"("]],"a":[]})";
        const EventProcessResult result = robust_process(
            processor,
            *state,
            batch,
            robust_context(
                PayloadVenue::spot,
                SpotStreamKind::depth_diff,
                index + 2U),
            payload);
        all_applied =
            all_applied && result.success() &&
            result.status == EventProcessStatus::applied_diff &&
            batch.has_audit_row && batch.has_order_book_row;
        all_rows_well_formed =
            all_rows_well_formed &&
            comma_count(batch.order_book_row.view()) == 25U &&
            !batch.order_book_row.empty() &&
            batch.order_book_row.view().back() == '\n';
    }
    context.expect(
        all_applied,
        "one thousand contiguous Spot diffs survive parser/buffer reuse");
    context.expect(
        all_rows_well_formed,
        "every long-chain snapshot retains the 26-column LF contract");
    context.expect(
        state->book_valid() &&
            state->last_update_id() == 1'100U &&
            state->sequence_number() == 1'001U &&
            state->last_connection_sequence() == 1'001U,
        "long Spot chain ends with exact update and sequence state");
}

void test_long_usdm_chain_and_precedence(Context& context) {
    std::optional<SymbolState> state =
        robust_state(context, PayloadVenue::usdm);
    if (!state) {
        return;
    }
    EventProcessor processor;
    EventRowBatch batch;
    constexpr std::string_view refresh =
        R"({"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":90,"u":100,"pu":89,"b":[["100","1"]],"a":[["101","1"]]})";
    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::usdm, SpotStreamKind::depth5, 1U),
        refresh));
    constexpr std::string_view bridge =
        R"({"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":100,"u":101,"pu":7,"b":[["100","2"]],"a":[]})";
    static_cast<void>(robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::usdm, SpotStreamKind::depth_diff, 2U),
        bridge));

    bool all_applied = true;
    std::uint64_t previous_update_id{101U};
    constexpr std::uint64_t event_count{500U};
    for (std::uint64_t index = 0; index < event_count; ++index) {
        const std::uint64_t final_update_id =
            previous_update_id + 100U;
        const std::string payload =
            R"({"e":"depthUpdate","E":2,"T":2,"s":"BTCUSDT","U":)" +
            std::to_string(final_update_id - 1U) +
            R"(,"u":)" + std::to_string(final_update_id) +
            R"(,"pu":)" + std::to_string(previous_update_id) +
            R"(,"b":[["100",")" +
            std::to_string(index + 3U) + R"("]],"a":[]})";
        const EventProcessResult result = robust_process(
            processor,
            *state,
            batch,
            robust_context(
                PayloadVenue::usdm,
                SpotStreamKind::depth_diff,
                index + 3U),
            payload);
        all_applied =
            all_applied &&
            result.status == EventProcessStatus::applied_diff &&
            batch.has_audit_row && batch.has_order_book_row;
        previous_update_id = final_update_id;
    }
    context.expect(
        all_applied,
        "five hundred USD-M diffs follow pu across non-overlapping U ranges");
    context.expect(
        state->book_valid() &&
            state->last_update_id() == previous_update_id &&
            state->sequence_number() == 502U,
        "long USD-M predecessor chain commits exact final state");

    const EventProcessResult redelivery = robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::usdm,
            SpotStreamKind::depth_diff,
            event_count + 3U),
        R"({"e":"depthUpdate","E":3,"T":3,"s":"BTCUSDT","U":100,"u":100,"pu":7,"b":[],"a":[]})");
    context.expect(
        redelivery.status == EventProcessStatus::sequence_gap &&
            redelivery.book_status == BookApplyStatus::sequence_gap &&
            batch.has_audit_row && !batch.has_order_book_row &&
            !state->book_valid() &&
            state->sequence_number() == 502U,
        "post-bridge mismatched pu invalidates before stale classification");

    const EventProcessResult recovery = robust_process(
        processor,
        *state,
        batch,
        robust_context(
            PayloadVenue::usdm,
            SpotStreamKind::depth5,
            event_count + 4U),
        R"({"e":"depthUpdate","E":4,"T":4,"s":"BTCUSDT","U":40,"u":50,"pu":39,"b":[["80","1"]],"a":[["81","1"]]})");
    context.expect(
        recovery.status == EventProcessStatus::applied_refresh &&
            state->book_valid() &&
            state->last_update_id() == 50U &&
            state->sequence_number() == 503U,
        "lower USD-M refresh recovers after chain invalidation");
}

struct ReplayFixtureEvent {
    SpotStreamKind kind;
    std::string_view payload;
};

void test_multi_event_live_replay_equivalence(Context& context) {
    std::optional<SymbolState> live =
        robust_state(context, PayloadVenue::spot);
    std::optional<SymbolState> replay =
        robust_state(context, PayloadVenue::spot);
    if (!live || !replay) {
        return;
    }
    EventProcessor live_processor;
    EventProcessor replay_processor;
    EventRowBatch live_batch;
    EventRowBatch replay_batch;
    constexpr std::array<ReplayFixtureEvent, 7U> events{{
        {
            SpotStreamKind::depth5,
            R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})",
        },
        {
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100","2"]],"a":[]})",
        },
        {
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":3,"s":"BTCUSDT","U":101,"u":101,"b":[],"a":[]})",
        },
        {
            SpotStreamKind::trade,
            R"({"e":7,"s":"ETHUSDT"})",
        },
        {
            SpotStreamKind::depth5,
            R"({"lastUpdateId":102,"bids":[["100","3"]]})",
        },
        {
            SpotStreamKind::depth_diff,
            R"({"e":"depthUpdate","E":4,"s":"BTCUSDT","U":105,"u":105,"b":[],"a":[]})",
        },
        {
            SpotStreamKind::depth5,
            R"({"lastUpdateId":50,"bids":[["80","1"]],"asks":[["81","1"]]})",
        },
    }};

    bool equivalent = true;
    for (std::size_t index = 0; index < events.size(); ++index) {
        const EventContext event = robust_context(
            PayloadVenue::spot,
            events[index].kind,
            static_cast<std::uint64_t>(index) + 1U);
        const EventProcessResult live_result = robust_process(
            live_processor,
            *live,
            live_batch,
            event,
            events[index].payload,
            EventOutputMode::live_capture);
        const EventProcessResult replay_result = robust_process(
            replay_processor,
            *replay,
            replay_batch,
            event,
            events[index].payload,
            EventOutputMode::replay);
        equivalent =
            equivalent &&
            live_result.status == replay_result.status &&
            live_result.error == replay_result.error &&
            live_result.parse_error == replay_result.parse_error &&
            live_batch.has_audit_row &&
            !replay_batch.has_audit_row &&
            live_batch.has_order_book_row ==
                replay_batch.has_order_book_row &&
            (!live_batch.has_order_book_row ||
             live_batch.order_book_row.view() ==
                 replay_batch.order_book_row.view()) &&
            live->book_valid() == replay->book_valid() &&
            live->last_update_id() == replay->last_update_id() &&
            live->sequence_number() == replay->sequence_number();
    }
    context.expect(
        equivalent,
        "multi-event live/replay sequence remains byte/state equivalent");
    context.expect(
        live->book_valid() &&
            live->last_update_id() == 50U &&
            live->sequence_number() == 3U,
        "equivalence fixture covers gap invalidation and lower recovery");
}

void test_interleaved_symbols_share_connection_sequence(
    Context& context) {
    std::optional<SymbolState> bitcoin =
        SymbolState::create(PayloadVenue::spot, "BTCUSDT");
    std::optional<SymbolState> ether =
        SymbolState::create(PayloadVenue::spot, "ETHUSDT");
    context.expect(
        bitcoin.has_value() && ether.has_value(),
        "two independent symbols initialize");
    if (!bitcoin || !ether) {
        return;
    }

    EventProcessor processor;
    EventRowBatch batch;
    const EventProcessResult bitcoin_refresh = robust_process(
        processor,
        *bitcoin,
        batch,
        robust_context(
            PayloadVenue::spot,
            SpotStreamKind::depth5,
            1U,
            0U,
            "BTCUSDT"),
        R"({"lastUpdateId":100,"bids":[["100","1"]],"asks":[["101","1"]]})");
    const bool bitcoin_id_written =
        batch.order_book_row.view().find(",1747767916,P,N,") !=
        std::string_view::npos;

    const EventProcessResult ether_refresh = robust_process(
        processor,
        *ether,
        batch,
        robust_context(
            PayloadVenue::spot,
            SpotStreamKind::depth5,
            2U,
            0U,
            "ETHUSDT"),
        R"({"lastUpdateId":200,"bids":[["200","1"]],"asks":[["201","1"]]})");
    const bool ether_id_written =
        batch.order_book_row.view().find(",1617942128,P,N,") !=
        std::string_view::npos;

    const EventProcessResult bitcoin_diff = robust_process(
        processor,
        *bitcoin,
        batch,
        robust_context(
            PayloadVenue::spot,
            SpotStreamKind::depth_diff,
            3U,
            0U,
            "BTCUSDT"),
        R"({"e":"depthUpdate","E":2,"s":"BTCUSDT","U":101,"u":101,"b":[["100","2"]],"a":[]})");
    const EventProcessResult ether_diff = robust_process(
        processor,
        *ether,
        batch,
        robust_context(
            PayloadVenue::spot,
            SpotStreamKind::depth_diff,
            4U,
            0U,
            "ETHUSDT"),
        R"({"e":"depthUpdate","E":2,"s":"ETHUSDT","U":201,"u":201,"b":[["200","2"]],"a":[]})");

    context.expect(
        bitcoin_refresh.status ==
                EventProcessStatus::applied_refresh &&
            ether_refresh.status ==
                EventProcessStatus::applied_refresh &&
            bitcoin_diff.status ==
                EventProcessStatus::applied_diff &&
            ether_diff.status ==
                EventProcessStatus::applied_diff,
        "alternating symbols dispatch through one reusable processor");
    context.expect(
        bitcoin_id_written && ether_id_written,
        "each symbol emits its own stable instrument ID");
    context.expect(
        bitcoin->sequence_number() == 2U &&
            ether->sequence_number() == 2U &&
            bitcoin->last_connection_sequence() == 3U &&
            ether->last_connection_sequence() == 4U,
        "per-symbol seqNo is contiguous while conn_seq retains global gaps");
}

[[nodiscard]] std::int32_t reference_instrument_id(
    const std::string_view symbol) noexcept {
    std::uint32_t hash{2'166'136'261U};
    for (const char byte : symbol) {
        hash ^= static_cast<std::uint8_t>(byte);
        hash *= 16'777'619U;
    }
    std::uint32_t value = hash & 0x7fff'ffffU;
    if (value == 0U) {
        value = 1U;
    }
    return static_cast<std::int32_t>(value);
}

void test_identity_generated_corpus(Context& context) {
    bool corpus_matches = true;
    for (std::uint32_t index = 0; index < 1000U; ++index) {
        const std::string symbol = "S" + std::to_string(index);
        const InstrumentIdResult actual =
            derive_instrument_id(symbol);
        corpus_matches =
            corpus_matches && actual.has_value() &&
            actual.value == reference_instrument_id(symbol);
    }
    context.expect(
        corpus_matches,
        "one thousand generated symbols match independent FNV reference");

    std::array<std::string, kMaxConfiguredSymbols> owned{};
    std::array<std::string_view, kMaxConfiguredSymbols> views{};
    for (std::size_t index = 0; index < owned.size(); ++index) {
        owned[index] = "S" + std::to_string(index);
        views[index] = owned[index];
    }
    context.expect(
        validate_symbol_set(views.data(), views.size()).has_value(),
        "exact 32-symbol configuration boundary is accepted");
    owned.back() = "bad";
    views.back() = owned.back();
    const SymbolSetValidationResult invalid_last =
        validate_symbol_set(views.data(), views.size());
    context.expect(
        invalid_last.error == SymbolValidationError::invalid_symbol &&
            invalid_last.first_index == kMaxConfiguredSymbols - 1U,
        "invalid symbol at final configuration slot reports exact index");
}

}  // namespace

void run_event_processor_robustness_tests(Context& context) {
    test_fatal_paths_are_atomic(context);
    test_pre_audit_policy_matrix(context);
    test_long_spot_chain(context);
    test_long_usdm_chain_and_precedence(context);
    test_multi_event_live_replay_equivalence(context);
    test_interleaved_symbols_share_connection_sequence(context);
    test_identity_generated_corpus(context);
}

}  // namespace hft::test
