#include "hft/command_line.h"

#include "hft/symbol_identity.h"

#include <array>
#include <charconv>
#include <new>
#include <system_error>

namespace hft {
namespace {

[[nodiscard]] bool option_value(
    const int argc,
    const char* const* const argv,
    int& index,
    std::string_view& value) noexcept {
    if (index + 1 >= argc || argv[index + 1] == nullptr) {
        return false;
    }
    ++index;
    value = argv[index];
    return !value.empty();
}

[[nodiscard]] bool normalize_symbol(
    const std::string_view input,
    std::string& output) {
    if (input.empty() ||
        input.size() > kMaxNormalizedSymbolBytes) {
        return false;
    }
    output.clear();
    output.reserve(input.size());
    for (const char byte : input) {
        if (byte >= 'a' && byte <= 'z') {
            output.push_back(
                static_cast<char>(byte - 'a' + 'A'));
        } else if ((byte >= 'A' && byte <= 'Z') ||
                   (byte >= '0' && byte <= '9')) {
            output.push_back(byte);
        } else {
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool parse_symbols(
    const std::string_view value,
    std::vector<std::string>& symbols) {
    symbols.clear();
    std::size_t begin = 0;
    while (begin <= value.size()) {
        const std::size_t comma = value.find(',', begin);
        const std::size_t end =
            comma == std::string_view::npos ? value.size() : comma;
        if (symbols.size() == kMaxConfiguredSymbols) {
            return false;
        }
        std::string normalized;
        if (!normalize_symbol(
                value.substr(begin, end - begin), normalized)) {
            return false;
        }
        symbols.push_back(std::move(normalized));
        if (comma == std::string_view::npos) {
            break;
        }
        begin = comma + 1U;
    }

    std::array<std::string_view, kMaxConfiguredSymbols> views{};
    for (std::size_t index = 0; index < symbols.size(); ++index) {
        views[index] = symbols[index];
    }
    return validate_symbol_set(views.data(), symbols.size()).has_value();
}

[[nodiscard]] bool parse_positive_integer(
    const std::string_view value,
    std::uint64_t& output) noexcept {
    if (value.empty() || value.front() == '+' ||
        value.front() == '-') {
        return false;
    }
    output = 0;
    const auto parsed = std::from_chars(
        value.data(), value.data() + value.size(), output);
    return parsed.ec == std::errc{} &&
           parsed.ptr == value.data() + value.size() && output != 0U;
}

CommandLineParseResult parse_command_line_impl(
    const int argc,
    const char* const* const argv) {
    CommandLineParseResult result;
    if (argc <= 1 || argv == nullptr) {
        result.error = CommandLineErrorCode::missing_mode;
        return result;
    }

    bool help_seen = false;
    bool venue_seen = false;
    bool symbols_seen = false;
    bool output_seen = false;
    bool duration_seen = false;
    for (int index = 1; index < argc; ++index) {
        if (argv[index] == nullptr) {
            result.error = CommandLineErrorCode::unknown_argument;
            result.argument_index = static_cast<std::size_t>(index);
            return result;
        }
        const std::string_view argument{argv[index]};
        std::string_view value;
        if (argument == "--help") {
            if (help_seen) {
                result.error = CommandLineErrorCode::duplicate_option;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            help_seen = true;
        } else if (argument == "--venue") {
            if (venue_seen) {
                result.error = CommandLineErrorCode::duplicate_option;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            venue_seen = true;
            if (!option_value(argc, argv, index, value)) {
                result.error = CommandLineErrorCode::missing_value;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            if (value == "spot") {
                result.options.venue = PayloadVenue::spot;
            } else if (value == "usdm") {
                result.options.venue = PayloadVenue::usdm;
            } else {
                result.error = CommandLineErrorCode::invalid_venue;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
        } else if (argument == "--symbols") {
            if (symbols_seen) {
                result.error = CommandLineErrorCode::duplicate_option;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            symbols_seen = true;
            if (!option_value(argc, argv, index, value)) {
                result.error = CommandLineErrorCode::missing_value;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            if (!parse_symbols(value, result.options.symbols)) {
                result.error = CommandLineErrorCode::invalid_symbols;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
        } else if (argument == "--duration") {
            if (duration_seen) {
                result.error = CommandLineErrorCode::duplicate_option;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            duration_seen = true;
            if (!option_value(argc, argv, index, value)) {
                result.error = CommandLineErrorCode::missing_value;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            std::uint64_t duration = 0;
            if (!parse_positive_integer(value, duration)) {
                result.error = CommandLineErrorCode::invalid_duration;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            result.options.duration_seconds = duration;
        } else if (argument == "--output-dir") {
            if (output_seen) {
                result.error = CommandLineErrorCode::duplicate_option;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            output_seen = true;
            if (!option_value(argc, argv, index, value)) {
                result.error = CommandLineErrorCode::missing_value;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            result.options.output_directory = value;
        } else if (argument == "--replay") {
            if (!option_value(argc, argv, index, value)) {
                result.error = CommandLineErrorCode::missing_value;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            if (result.options.replay_inputs.size() ==
                kMaxConfiguredSymbols) {
                result.error =
                    CommandLineErrorCode::too_many_replay_inputs;
                result.argument_index = static_cast<std::size_t>(index);
                return result;
            }
            for (const std::string& existing :
                 result.options.replay_inputs) {
                if (existing == value) {
                    result.error =
                        CommandLineErrorCode::duplicate_replay_input;
                    result.argument_index =
                        static_cast<std::size_t>(index);
                    return result;
                }
            }
            result.options.replay_inputs.emplace_back(value);
        } else {
            result.error = CommandLineErrorCode::unknown_argument;
            result.argument_index = static_cast<std::size_t>(index);
            return result;
        }
    }

    if (help_seen) {
        if (argc != 2) {
            result.error =
                CommandLineErrorCode::help_with_other_options;
            return result;
        }
        result.options.mode = ApplicationMode::help;
        return result;
    }
    if (!result.options.replay_inputs.empty()) {
        if (venue_seen || symbols_seen || duration_seen) {
            result.error = CommandLineErrorCode::mixed_modes;
            return result;
        }
        if (!output_seen) {
            result.error =
                CommandLineErrorCode::missing_replay_output;
            return result;
        }
        result.options.mode = ApplicationMode::replay;
        return result;
    }
    if (!venue_seen && !symbols_seen && !duration_seen) {
        result.error = CommandLineErrorCode::missing_mode;
        return result;
    }
    if (!venue_seen || !symbols_seen) {
        result.error = CommandLineErrorCode::missing_live_option;
        return result;
    }
    result.options.mode = ApplicationMode::live_capture;
    return result;
}

}  // namespace

CommandLineParseResult parse_command_line(
    const int argc,
    const char* const* const argv) noexcept {
    try {
        return parse_command_line_impl(argc, argv);
    } catch (const std::bad_alloc&) {
        CommandLineParseResult result;
        result.error = CommandLineErrorCode::allocation_failure;
        return result;
    }
}

}  // namespace hft
