#pragma once

#include "hft/spot_payload_parser.h"

#include <simdjson.h>

#include <cstddef>
#include <string_view>
#include <utility>

namespace hft::detail {

[[nodiscard]] inline bool is_json_digit(const char value) noexcept {
    return value >= '0' && value <= '9';
}

[[nodiscard]] inline bool is_valid_json_number_token(
    const std::string_view token) noexcept {
    std::size_t index = 0;
    if (index < token.size() && token[index] == '-') {
        ++index;
    }
    if (index == token.size()) {
        return false;
    }
    if (token[index] == '0') {
        ++index;
        if (index < token.size() && is_json_digit(token[index])) {
            return false;
        }
    } else {
        if (token[index] < '1' || token[index] > '9') {
            return false;
        }
        do {
            ++index;
        } while (index < token.size() && is_json_digit(token[index]));
    }
    if (index < token.size() && token[index] == '.') {
        ++index;
        const std::size_t start = index;
        while (index < token.size() && is_json_digit(token[index])) {
            ++index;
        }
        if (index == start) {
            return false;
        }
    }
    if (index < token.size() &&
        (token[index] == 'e' || token[index] == 'E')) {
        ++index;
        if (index < token.size() &&
            (token[index] == '+' || token[index] == '-')) {
            ++index;
        }
        const std::size_t start = index;
        while (index < token.size() && is_json_digit(token[index])) {
            ++index;
        }
        if (index == start) {
            return false;
        }
    }
    return index == token.size();
}

[[nodiscard]] inline std::string_view trim_json_whitespace(
    std::string_view input) noexcept {
    const auto is_whitespace = [](const char value) noexcept {
        return value == ' ' || value == '\t' || value == '\n' ||
               value == '\r';
    };
    while (!input.empty() && is_whitespace(input.front())) {
        input.remove_prefix(1U);
    }
    while (!input.empty() && is_whitespace(input.back())) {
        input.remove_suffix(1U);
    }
    return input;
}

[[nodiscard]] inline simdjson::error_code validate_json_value(
    simdjson::ondemand::value& value,
    const std::size_t container_depth) noexcept {
    simdjson::ondemand::json_type type;
    simdjson::error_code error = value.type().get(type);
    if (error != simdjson::SUCCESS) {
        return error;
    }
    switch (type) {
        case simdjson::ondemand::json_type::array: {
            if (container_depth > kMaxJsonNestingDepth) {
                return simdjson::DEPTH_ERROR;
            }
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
                error =
                    validate_json_value(child, container_depth + 1U);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
            }
            return simdjson::SUCCESS;
        }
        case simdjson::ondemand::json_type::object: {
            if (container_depth > kMaxJsonNestingDepth) {
                return simdjson::DEPTH_ERROR;
            }
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
                error = validate_json_value(
                    field.value(), container_depth + 1U);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
            }
            return simdjson::SUCCESS;
        }
        case simdjson::ondemand::json_type::number:
            return is_valid_json_number_token(
                       value.raw_json_token())
                       ? simdjson::SUCCESS
                       : simdjson::NUMBER_ERROR;
        case simdjson::ondemand::json_type::string: {
            std::string_view decoded;
            return value.get_string().get(decoded);
        }
        case simdjson::ondemand::json_type::boolean: {
            bool decoded = false;
            return value.get_bool().get(decoded);
        }
        case simdjson::ondemand::json_type::null:
            return value.raw_json_token() == "null"
                       ? simdjson::SUCCESS
                       : simdjson::N_ATOM_ERROR;
    }
    return simdjson::UNEXPECTED_ERROR;
}

[[nodiscard]] inline simdjson::error_code
validate_non_object_document(
    simdjson::ondemand::document& document,
    const simdjson::ondemand::json_type type,
    const std::string_view source,
    const std::size_t child_container_depth) noexcept {
    switch (type) {
        case simdjson::ondemand::json_type::array: {
            simdjson::ondemand::array array;
            simdjson::error_code error =
                document.get_array().get(array);
            if (error != simdjson::SUCCESS) {
                return error;
            }
            for (auto child_result : array) {
                simdjson::ondemand::value child;
                error = std::move(child_result).get(child);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
                error = validate_json_value(
                    child, child_container_depth);
                if (error != simdjson::SUCCESS) {
                    return error;
                }
            }
            return simdjson::SUCCESS;
        }
        case simdjson::ondemand::json_type::number:
            return is_valid_json_number_token(
                       trim_json_whitespace(source))
                       ? simdjson::SUCCESS
                       : simdjson::NUMBER_ERROR;
        case simdjson::ondemand::json_type::string: {
            std::string_view decoded;
            return document.get_string(false).get(decoded);
        }
        case simdjson::ondemand::json_type::boolean: {
            bool decoded = false;
            return document.get_bool().get(decoded);
        }
        case simdjson::ondemand::json_type::null:
            return trim_json_whitespace(source) == "null"
                       ? simdjson::SUCCESS
                       : simdjson::N_ATOM_ERROR;
        case simdjson::ondemand::json_type::object:
            return simdjson::UNEXPECTED_ERROR;
    }
    return simdjson::UNEXPECTED_ERROR;
}

}  // namespace hft::detail
