#include "hft/spot_payload_parser.h"

#include <simdjson.h>

#include <array>
#include <limits>
#include <stdexcept>
#include <utility>

namespace hft {
namespace {

constexpr std::size_t kDuplicateTableSize{32'768U};
constexpr std::size_t kDuplicateTableMask{kDuplicateTableSize - 1U};

static_assert(kJsonPaddingBytes == simdjson::SIMDJSON_PADDING);
static_assert((kDuplicateTableSize & kDuplicateTableMask) == 0U);
static_assert(kDuplicateTableSize >= (kMaxDepthUpdates * 2U));

struct ParseFailure {
    SpotParseError error{SpotParseError::none};
    SpotField field{SpotField::none};
    DecimalParseError decimal_error{DecimalParseError::none};

    [[nodiscard]] bool failed() const noexcept {
        return error != SpotParseError::none;
    }
};

[[nodiscard]] ParseFailure fail(
    const SpotParseError error,
    const SpotField field,
    const DecimalParseError decimal_error = DecimalParseError::none) noexcept {
    return ParseFailure{error, field, decimal_error};
}

[[nodiscard]] bool equals_ascii_case_insensitive(
    const std::string_view lhs,
    const std::string_view rhs) noexcept {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    for (std::size_t index = 0; index < lhs.size(); ++index) {
        const auto normalize = [](const char value) noexcept {
            if (value >= 'a' && value <= 'z') {
                return static_cast<char>(value - ('a' - 'A'));
            }
            return value;
        };
        if (normalize(lhs[index]) != normalize(rhs[index])) {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool mark_once(
    std::uint32_t& fields,
    const std::uint32_t field) noexcept {
    if ((fields & field) != 0U) {
        return false;
    }
    fields |= field;
    return true;
}

[[nodiscard]] bool is_schema_conversion_error(
    const simdjson::error_code error) noexcept {
    return error == simdjson::INCORRECT_TYPE ||
           error == simdjson::NUMBER_OUT_OF_RANGE;
}

[[nodiscard]] std::uint64_t mix_key(
    const std::int64_t price,
    const BookSide side) noexcept {
    auto value = static_cast<std::uint64_t>(price);
    value ^= side == BookSide::bid ? 0x9e3779b97f4a7c15ULL
                                  : 0xd1b54a32d192ed03ULL;
    value ^= value >> 30U;
    value *= 0xbf58476d1ce4e5b9ULL;
    value ^= value >> 27U;
    value *= 0x94d049bb133111ebULL;
    value ^= value >> 31U;
    return value;
}

[[nodiscard]] simdjson::error_code validate_json_value(
    simdjson::ondemand::value& value) noexcept {
    simdjson::ondemand::json_type type;
    simdjson::error_code error = value.type().get(type);
    if (error != simdjson::SUCCESS) {
        return error;
    }

    switch (type) {
        case simdjson::ondemand::json_type::array: {
            simdjson::ondemand::array array;
            error = value.get_array().get(array);
            if (error != simdjson::SUCCESS) {
                return error;
            }
            for (auto child_result : array) {
                simdjson::ondemand::value child;
                error = std::move(child_result).get(child);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
                error = validate_json_value(child);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
            }
            return simdjson::SUCCESS;
        }
        case simdjson::ondemand::json_type::object: {
            simdjson::ondemand::object object;
            error = value.get_object().get(object);
            if (error != simdjson::SUCCESS) {
                return error;
            }
            for (auto field_result : object) {
                simdjson::ondemand::field field;
                error = std::move(field_result).get(field);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
                std::string_view key;
                error = field.unescaped_key(false).get(key);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
                static_cast<void>(key);
                error = validate_json_value(field.value());
                if (error != simdjson::SUCCESS) {
                    return error;
                }
            }
            return simdjson::SUCCESS;
        }
        case simdjson::ondemand::json_type::number: {
            simdjson::ondemand::number number;
            return value.get_number().get(number);
        }
        case simdjson::ondemand::json_type::string: {
            std::string_view string;
            return value.get_string().get(string);
        }
        case simdjson::ondemand::json_type::boolean: {
            bool boolean{false};
            return value.get_bool().get(boolean);
        }
        case simdjson::ondemand::json_type::null: {
            bool is_null{false};
            error = value.is_null().get(is_null);
            return error == simdjson::SUCCESS && is_null
                       ? simdjson::SUCCESS
                       : error;
        }
    }
    return simdjson::UNEXPECTED_ERROR;
}

[[nodiscard]] ParseFailure read_json_string(
    simdjson::ondemand::value& value,
    std::string_view& output,
    const SpotField field) noexcept {
    simdjson::ondemand::json_type type;
    simdjson::error_code error = value.type().get(type);
    if (error != simdjson::SUCCESS) {
        return fail(SpotParseError::malformed_json, SpotField::root);
    }
    if (type != simdjson::ondemand::json_type::string) {
        error = validate_json_value(value);
        return error == simdjson::SUCCESS
                   ? fail(SpotParseError::wrong_type, field)
                   : fail(
                         SpotParseError::malformed_json,
                         SpotField::root);
    }
    error = value.get_string().get(output);
    return error == simdjson::SUCCESS
               ? ParseFailure{}
               : fail(SpotParseError::malformed_json, SpotField::root);
}

[[nodiscard]] ParseFailure read_json_uint64(
    simdjson::ondemand::value& value,
    std::uint64_t& output,
    const SpotField field) noexcept {
    simdjson::ondemand::json_type type;
    simdjson::error_code error = value.type().get(type);
    if (error != simdjson::SUCCESS) {
        return fail(SpotParseError::malformed_json, SpotField::root);
    }
    if (type != simdjson::ondemand::json_type::number) {
        error = validate_json_value(value);
        return error == simdjson::SUCCESS
                   ? fail(SpotParseError::wrong_type, field)
                   : fail(
                         SpotParseError::malformed_json,
                         SpotField::root);
    }

    error = value.get_uint64().get(output);
    if (error == simdjson::SUCCESS) {
        return {};
    }
    return is_schema_conversion_error(error)
               ? fail(SpotParseError::wrong_type, field)
               : fail(SpotParseError::malformed_json, SpotField::root);
}

}  // namespace

struct SpotPayloadParser::Impl {
    enum class InsertResult : std::uint8_t {
        inserted,
        duplicate,
        table_full,
    };

    struct DuplicateSlot {
        std::uint64_t generation{0};
        std::int64_t price{0};
        BookSide side{BookSide::bid};
    };

    simdjson::ondemand::parser parser{};
    std::array<LevelUpdate, kMaxDepthUpdates> updates{};
    std::array<DuplicateSlot, kDuplicateTableSize> duplicates{};
    std::uint64_t generation{0};
    std::size_t update_count{0};
    std::size_t encountered_levels{0};

    Impl() {
        const simdjson::error_code error = parser.allocate(kMaxPayloadBytes);
        if (error != simdjson::SUCCESS) {
            throw std::runtime_error{"unable to preallocate Spot JSON parser"};
        }
    }

    void begin_message() noexcept {
        update_count = 0;
        encountered_levels = 0;
        if (generation == std::numeric_limits<std::uint64_t>::max()) {
            for (DuplicateSlot& slot : duplicates) {
                slot.generation = 0;
            }
            generation = 1;
            return;
        }
        ++generation;
    }

    [[nodiscard]] InsertResult insert_unique(
        const std::int64_t price,
        const BookSide side) noexcept {
        std::size_t slot_index =
            static_cast<std::size_t>(mix_key(price, side)) &
            kDuplicateTableMask;

        for (std::size_t probe = 0; probe < kDuplicateTableSize; ++probe) {
            DuplicateSlot& slot = duplicates[slot_index];
            if (slot.generation != generation) {
                slot.generation = generation;
                slot.price = price;
                slot.side = side;
                return InsertResult::inserted;
            }
            if (slot.price == price && slot.side == side) {
                return InsertResult::duplicate;
            }
            slot_index = (slot_index + 1U) & kDuplicateTableMask;
        }
        return InsertResult::table_full;
    }

    [[nodiscard]] ParseFailure parse_levels(
        simdjson::ondemand::value& value,
        const BookSide side,
        const bool is_partial,
        LevelUpdateRange& output) noexcept {
        const SpotField side_field =
            side == BookSide::bid ? SpotField::bids : SpotField::asks;
        ParseFailure first_error;
        const auto record_error = [&first_error](
                                      const SpotParseError error,
                                      const SpotField field,
                                      const DecimalParseError
                                          decimal_error =
                                              DecimalParseError::none)
            noexcept {
            if (!first_error.failed()) {
                first_error = fail(error, field, decimal_error);
            }
        };

        simdjson::ondemand::json_type levels_type;
        simdjson::error_code json_error =
            value.type().get(levels_type);
        if (json_error != simdjson::SUCCESS) {
            return fail(SpotParseError::malformed_json, SpotField::root);
        }
        if (levels_type != simdjson::ondemand::json_type::array) {
            json_error = validate_json_value(value);
            if (json_error != simdjson::SUCCESS) {
                return fail(
                    SpotParseError::malformed_json, SpotField::root);
            }
            return fail(SpotParseError::wrong_type, side_field);
        }

        simdjson::ondemand::array levels;
        json_error = value.get_array().get(levels);
        if (json_error != simdjson::SUCCESS) {
            return fail(SpotParseError::malformed_json, SpotField::root);
        }

        const std::size_t start = update_count;
        std::size_t side_level_count{0};
        std::int64_t previous_price{0};
        bool has_previous_price{false};

        for (auto level_result : levels) {
            simdjson::ondemand::value level_value;
            json_error = std::move(level_result).get(level_value);
            if (json_error != simdjson::SUCCESS) {
                return fail(
                    SpotParseError::malformed_json, SpotField::root);
            }

            const bool over_capacity =
                encountered_levels >= kMaxDepthUpdates ||
                (is_partial &&
                 side_level_count >= kMaxPartialDepthLevelsPerSide);
            ++encountered_levels;
            ++side_level_count;
            if (over_capacity) {
                record_error(SpotParseError::too_many_levels, side_field);
                json_error = validate_json_value(level_value);
                if (json_error != simdjson::SUCCESS) {
                    return fail(
                        SpotParseError::malformed_json,
                        SpotField::root);
                }
                continue;
            }

            simdjson::ondemand::json_type level_type;
            json_error = level_value.type().get(level_type);
            if (json_error != simdjson::SUCCESS) {
                return fail(
                    SpotParseError::malformed_json, SpotField::root);
            }
            if (level_type != simdjson::ondemand::json_type::array) {
                record_error(
                    SpotParseError::invalid_level_shape,
                    SpotField::level);
                json_error = validate_json_value(level_value);
                if (json_error != simdjson::SUCCESS) {
                    return fail(
                        SpotParseError::malformed_json,
                        SpotField::root);
                }
                continue;
            }

            simdjson::ondemand::array pair;
            json_error = level_value.get_array().get(pair);
            if (json_error != simdjson::SUCCESS) {
                return fail(
                    SpotParseError::malformed_json, SpotField::root);
            }

            std::string_view price_text;
            std::string_view quantity_text;
            std::size_t component_count{0};
            bool components_valid{true};
            for (auto component_result : pair) {
                simdjson::ondemand::value component_value;
                json_error =
                    std::move(component_result).get(component_value);
                if (json_error != simdjson::SUCCESS) {
                    return fail(
                        SpotParseError::malformed_json,
                        SpotField::root);
                }

                if (component_count >= 2U) {
                    record_error(
                        SpotParseError::invalid_level_shape,
                        SpotField::level);
                    components_valid = false;
                    json_error = validate_json_value(component_value);
                    if (json_error != simdjson::SUCCESS) {
                        return fail(
                            SpotParseError::malformed_json,
                            SpotField::root);
                    }
                    ++component_count;
                    continue;
                }

                simdjson::ondemand::json_type component_type;
                json_error = component_value.type().get(component_type);
                if (json_error != simdjson::SUCCESS) {
                    return fail(
                        SpotParseError::malformed_json,
                        SpotField::root);
                }
                if (component_type !=
                    simdjson::ondemand::json_type::string) {
                    record_error(
                        SpotParseError::wrong_type,
                        component_count == 0U ? SpotField::price
                                              : SpotField::quantity);
                    components_valid = false;
                    json_error = validate_json_value(component_value);
                    if (json_error != simdjson::SUCCESS) {
                        return fail(
                            SpotParseError::malformed_json,
                            SpotField::root);
                    }
                    ++component_count;
                    continue;
                }

                std::string_view component;
                json_error =
                    component_value.get_string().get(component);
                if (json_error != simdjson::SUCCESS) {
                    return fail(
                        SpotParseError::malformed_json,
                        SpotField::root);
                }
                if (component_count == 0U) {
                    price_text = component;
                } else {
                    quantity_text = component;
                }
                ++component_count;
            }

            if (component_count != 2U) {
                record_error(
                    SpotParseError::invalid_level_shape,
                    SpotField::level);
                components_valid = false;
            }
            if (!components_valid) {
                continue;
            }

            const DecimalParseResult parsed_price =
                parse_decimal_1e8(price_text);
            if (!parsed_price) {
                record_error(
                    SpotParseError::invalid_decimal,
                    SpotField::price,
                    parsed_price.error);
                continue;
            }
            if (parsed_price.value <= 0) {
                record_error(
                    SpotParseError::non_positive_price,
                    SpotField::price);
                continue;
            }

            const DecimalParseResult parsed_quantity =
                parse_decimal_1e8(quantity_text);
            if (!parsed_quantity) {
                record_error(
                    SpotParseError::invalid_decimal,
                    SpotField::quantity,
                    parsed_quantity.error);
                continue;
            }
            if (is_partial && parsed_quantity.value <= 0) {
                record_error(
                    SpotParseError::invalid_quantity,
                    SpotField::quantity);
                continue;
            }

            const InsertResult insert_result =
                insert_unique(parsed_price.value, side);
            if (insert_result == InsertResult::duplicate) {
                record_error(
                    SpotParseError::duplicate_price,
                    SpotField::price);
                continue;
            }
            if (insert_result == InsertResult::table_full) {
                return fail(
                    SpotParseError::internal_capacity_error,
                    SpotField::level);
            }

            if (is_partial && has_previous_price) {
                const bool correctly_ordered =
                    side == BookSide::bid
                        ? previous_price > parsed_price.value
                        : previous_price < parsed_price.value;
                if (!correctly_ordered) {
                    record_error(
                        SpotParseError::invalid_level_order,
                        side_field);
                    continue;
                }
            }

            updates[update_count] =
                LevelUpdate{parsed_price.value, parsed_quantity.value, side};
            ++update_count;
            previous_price = parsed_price.value;
            has_previous_price = true;
        }

        output =
            LevelUpdateRange{updates.data() + start, update_count - start};
        return first_error;
    }

    [[nodiscard]] SpotParseResult parse_trade(
        simdjson::ondemand::object& object,
        const std::string_view expected_symbol) noexcept {
        SpotParseResult result;
        result.event.kind = SpotStreamKind::trade;

        for (auto field_result : object) {
            simdjson::ondemand::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) {
                result.error = SpotParseError::malformed_json;
                result.field = SpotField::root;
                return result;
            }

            std::string_view key;
            if (field.unescaped_key(false).get(key) !=
                simdjson::SUCCESS) {
                result.error = SpotParseError::malformed_json;
                result.field = SpotField::root;
                return result;
            }

            if (key == "e") {
                result.event.trade.event_type_present = true;
                std::string_view event_type;
                const ParseFailure value_error = read_json_string(
                    field.value(), event_type, SpotField::event_type);
                if (value_error.error ==
                    SpotParseError::malformed_json) {
                    result.error = SpotParseError::malformed_json;
                    result.field = SpotField::root;
                    return result;
                }
                result.event.trade.event_type_matches =
                    result.event.trade.event_type_matches &&
                    !value_error.failed() &&
                    event_type == "trade";
            } else if (key == "s") {
                result.event.trade.symbol_present = true;
                std::string_view symbol;
                const ParseFailure value_error = read_json_string(
                    field.value(), symbol, SpotField::symbol);
                if (value_error.error ==
                    SpotParseError::malformed_json) {
                    result.error = SpotParseError::malformed_json;
                    result.field = SpotField::root;
                    return result;
                }
                result.event.trade.symbol_matches =
                    result.event.trade.symbol_matches &&
                    !value_error.failed() &&
                    equals_ascii_case_insensitive(symbol, expected_symbol);
            } else {
                if (validate_json_value(field.value()) !=
                    simdjson::SUCCESS) {
                    result.error = SpotParseError::malformed_json;
                    result.field = SpotField::root;
                    return result;
                }
            }
        }
        return result;
    }

    [[nodiscard]] SpotParseResult parse_depth(
        simdjson::ondemand::object& object,
        const SpotStreamKind kind,
        const std::string_view expected_symbol) noexcept {
        constexpr std::uint32_t kEventTypeBit{1U << 0U};
        constexpr std::uint32_t kEventTimeBit{1U << 1U};
        constexpr std::uint32_t kSymbolBit{1U << 2U};
        constexpr std::uint32_t kFirstUpdateIdBit{1U << 3U};
        constexpr std::uint32_t kFinalUpdateIdBit{1U << 4U};
        constexpr std::uint32_t kLastUpdateIdBit{1U << 5U};
        constexpr std::uint32_t kBidsBit{1U << 6U};
        constexpr std::uint32_t kAsksBit{1U << 7U};
        constexpr std::uint32_t kDiffRequired{
            kEventTypeBit | kEventTimeBit | kSymbolBit |
            kFirstUpdateIdBit | kFinalUpdateIdBit | kBidsBit | kAsksBit};
        constexpr std::uint32_t kPartialRequired{
            kLastUpdateIdBit | kBidsBit | kAsksBit};

        SpotParseResult result;
        result.event.kind = kind;
        std::uint32_t fields{0};

        const auto record_error = [&result](
                                      const SpotParseError error,
                                      const SpotField field,
                                      const DecimalParseError
                                          decimal_error =
                                              DecimalParseError::none)
            noexcept {
            if (result.error == SpotParseError::none) {
                result.error = error;
                result.field = field;
                result.decimal_error = decimal_error;
            }
        };

        const auto apply_failure = [&record_error, &result](
                                       const ParseFailure& error) noexcept {
            if (!error.failed()) {
                return false;
            }
            if (error.error == SpotParseError::malformed_json) {
                result.error = error.error;
                result.field = error.field;
                result.decimal_error = error.decimal_error;
            } else {
                record_error(
                    error.error, error.field, error.decimal_error);
            }
            return true;
        };

        const auto duplicate = [&record_error, &fields, &result](
                                   const std::uint32_t bit,
                                   const SpotField field,
                                   simdjson::ondemand::value& value)
            noexcept {
            if (mark_once(fields, bit)) {
                return false;
            }
            record_error(SpotParseError::duplicate_field, field);
            if (validate_json_value(value) != simdjson::SUCCESS) {
                result.error = SpotParseError::malformed_json;
                result.field = SpotField::root;
                result.decimal_error = DecimalParseError::none;
            }
            return true;
        };

        for (auto field_result : object) {
            simdjson::ondemand::field field;
            if (std::move(field_result).get(field) != simdjson::SUCCESS) {
                result.error = SpotParseError::malformed_json;
                result.field = SpotField::root;
                return result;
            }

            std::string_view key;
            if (field.unescaped_key(false).get(key) !=
                simdjson::SUCCESS) {
                result.error = SpotParseError::malformed_json;
                result.field = SpotField::root;
                return result;
            }

            simdjson::ondemand::value& value = field.value();
            if (kind == SpotStreamKind::depth_diff && key == "e") {
                if (duplicate(
                        kEventTypeBit, SpotField::event_type, value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                std::string_view event_type;
                const ParseFailure value_error = read_json_string(
                    value, event_type, SpotField::event_type);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                if (event_type != "depthUpdate") {
                    record_error(
                        SpotParseError::unexpected_event_type,
                        SpotField::event_type);
                }
            } else if (
                kind == SpotStreamKind::depth_diff && key == "E") {
                if (duplicate(
                        kEventTimeBit, SpotField::event_time, value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure value_error = read_json_uint64(
                    value,
                    result.event.depth.event_time_ms,
                    SpotField::event_time);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                result.event.depth.has_event_time = true;
            } else if (
                kind == SpotStreamKind::depth_diff && key == "s") {
                if (duplicate(kSymbolBit, SpotField::symbol, value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                std::string_view symbol;
                const ParseFailure value_error = read_json_string(
                    value, symbol, SpotField::symbol);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                if (!equals_ascii_case_insensitive(
                        symbol, expected_symbol)) {
                    record_error(
                        SpotParseError::symbol_mismatch,
                        SpotField::symbol);
                }
            } else if (
                kind == SpotStreamKind::depth_diff && key == "U") {
                if (duplicate(
                        kFirstUpdateIdBit,
                        SpotField::first_update_id,
                        value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure value_error = read_json_uint64(
                    value,
                    result.event.depth.first_update_id,
                    SpotField::first_update_id);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
            } else if (
                kind == SpotStreamKind::depth_diff && key == "u") {
                if (duplicate(
                        kFinalUpdateIdBit,
                        SpotField::final_update_id,
                        value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure value_error = read_json_uint64(
                    value,
                    result.event.depth.final_update_id,
                    SpotField::final_update_id);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
            } else if (
                kind == SpotStreamKind::depth5 &&
                key == "lastUpdateId") {
                if (duplicate(
                        kLastUpdateIdBit,
                        SpotField::last_update_id,
                        value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure value_error = read_json_uint64(
                    value,
                    result.event.depth.final_update_id,
                    SpotField::last_update_id);
                if (apply_failure(value_error)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                result.event.depth.first_update_id =
                    result.event.depth.final_update_id;
            } else if (
                (kind == SpotStreamKind::depth_diff && key == "b") ||
                (kind == SpotStreamKind::depth5 && key == "bids")) {
                if (duplicate(kBidsBit, SpotField::bids, value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure error = parse_levels(
                    value,
                    BookSide::bid,
                    kind == SpotStreamKind::depth5,
                    result.event.depth.bids);
                if (error.failed()) {
                    if (error.error == SpotParseError::malformed_json ||
                        error.error ==
                            SpotParseError::internal_capacity_error) {
                        result.error = error.error;
                        result.field = error.field;
                        result.decimal_error = error.decimal_error;
                        return result;
                    }
                    record_error(
                        error.error, error.field, error.decimal_error);
                }
            } else if (
                (kind == SpotStreamKind::depth_diff && key == "a") ||
                (kind == SpotStreamKind::depth5 && key == "asks")) {
                if (duplicate(kAsksBit, SpotField::asks, value)) {
                    if (result.error == SpotParseError::malformed_json) {
                        return result;
                    }
                    continue;
                }
                const ParseFailure error = parse_levels(
                    value,
                    BookSide::ask,
                    kind == SpotStreamKind::depth5,
                    result.event.depth.asks);
                if (error.failed()) {
                    if (error.error == SpotParseError::malformed_json ||
                        error.error ==
                            SpotParseError::internal_capacity_error) {
                        result.error = error.error;
                        result.field = error.field;
                        result.decimal_error = error.decimal_error;
                        return result;
                    }
                    record_error(
                        error.error, error.field, error.decimal_error);
                }
            } else {
                if (validate_json_value(value) != simdjson::SUCCESS) {
                    result.error = SpotParseError::malformed_json;
                    result.field = SpotField::root;
                    result.decimal_error = DecimalParseError::none;
                    return result;
                }
            }
        }

        if (result.error != SpotParseError::none) {
            return result;
        }

        const std::uint32_t required =
            kind == SpotStreamKind::depth_diff ? kDiffRequired
                                                : kPartialRequired;
        if ((fields & required) != required) {
            result.error = SpotParseError::missing_field;
            if ((fields & kEventTypeBit) == 0U &&
                kind == SpotStreamKind::depth_diff) {
                result.field = SpotField::event_type;
            } else if (
                (fields & kEventTimeBit) == 0U &&
                kind == SpotStreamKind::depth_diff) {
                result.field = SpotField::event_time;
            } else if (
                (fields & kSymbolBit) == 0U &&
                kind == SpotStreamKind::depth_diff) {
                result.field = SpotField::symbol;
            } else if (
                (fields & kFirstUpdateIdBit) == 0U &&
                kind == SpotStreamKind::depth_diff) {
                result.field = SpotField::first_update_id;
            } else if (
                (fields & kFinalUpdateIdBit) == 0U &&
                kind == SpotStreamKind::depth_diff) {
                result.field = SpotField::final_update_id;
            } else if (
                (fields & kLastUpdateIdBit) == 0U &&
                kind == SpotStreamKind::depth5) {
                result.field = SpotField::last_update_id;
            } else if ((fields & kBidsBit) == 0U) {
                result.field = SpotField::bids;
            } else {
                result.field = SpotField::asks;
            }
            return result;
        }

        if (kind == SpotStreamKind::depth_diff &&
            result.event.depth.first_update_id >
                result.event.depth.final_update_id) {
            result.error = SpotParseError::invalid_update_range;
            result.field = SpotField::first_update_id;
            return result;
        }

        if (kind == SpotStreamKind::depth5 &&
            result.event.depth.bids.size != 0U &&
            result.event.depth.asks.size != 0U &&
            result.event.depth.bids[0].price >=
                result.event.depth.asks[0].price) {
            result.error = SpotParseError::crossed_book;
            result.field = SpotField::level;
            return result;
        }

        return result;
    }
};

SpotPayloadParser::SpotPayloadParser() : impl_{std::make_unique<Impl>()} {}

SpotPayloadParser::~SpotPayloadParser() = default;

SpotPayloadParser::SpotPayloadParser(SpotPayloadParser&&) noexcept = default;

SpotPayloadParser& SpotPayloadParser::operator=(
    SpotPayloadParser&&) noexcept = default;

SpotParseResult SpotPayloadParser::parse(
    const SpotStreamKind kind,
    const std::string_view expected_symbol,
    const PaddedJsonView payload) noexcept {
    SpotParseResult result;
    result.event.kind = kind;

    if (impl_ == nullptr || payload.data == nullptr ||
        payload.capacity < payload.size ||
        (payload.capacity - payload.size) < kJsonPaddingBytes) {
        result.error = SpotParseError::invalid_input_buffer;
        result.field = SpotField::root;
        return result;
    }
    if (payload.size > kMaxPayloadBytes) {
        result.error = SpotParseError::payload_too_large;
        result.field = SpotField::root;
        return result;
    }

    impl_->begin_message();

    simdjson::ondemand::document document;
    const simdjson::error_code iterate_error =
        impl_->parser
            .iterate(payload.data, payload.size, payload.capacity)
            .get(document);
    if (iterate_error != simdjson::SUCCESS) {
        result.error = SpotParseError::malformed_json;
        result.field = SpotField::root;
        return result;
    }

    simdjson::ondemand::object object;
    const simdjson::error_code object_error =
        document.get_object().get(object);
    if (object_error != simdjson::SUCCESS) {
        result.error = object_error == simdjson::INCORRECT_TYPE
                           ? SpotParseError::root_not_object
                           : SpotParseError::malformed_json;
        result.field = SpotField::root;
        return result;
    }

    if (kind == SpotStreamKind::trade) {
        return impl_->parse_trade(object, expected_symbol);
    }
    return impl_->parse_depth(object, kind, expected_symbol);
}

}  // namespace hft
