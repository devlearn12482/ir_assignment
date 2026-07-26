#pragma once

#include "hft/spot_payload_parser.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace hft {

enum class ApplicationMode : std::uint8_t {
    help,
    live_capture,
    replay,
};

enum class CommandLineErrorCode : std::uint8_t {
    none,
    allocation_failure,
    unknown_argument,
    missing_value,
    duplicate_option,
    invalid_venue,
    invalid_symbols,
    invalid_duration,
    invalid_output_directory,
    missing_replay_output,
    missing_mode,
    missing_live_option,
    mixed_modes,
    too_many_replay_inputs,
    duplicate_replay_input,
    help_with_other_options,
};

struct CommandLineOptions {
    ApplicationMode mode{ApplicationMode::help};
    PayloadVenue venue{PayloadVenue::spot};
    std::vector<std::string> symbols{};
    std::vector<std::string> replay_inputs{};
    std::string output_directory{"./output"};
    std::optional<std::uint64_t> duration_seconds{};
};

struct CommandLineParseResult {
    CommandLineOptions options{};
    CommandLineErrorCode error{CommandLineErrorCode::none};
    std::size_t argument_index{0};

    [[nodiscard]] bool success() const noexcept {
        return error == CommandLineErrorCode::none;
    }
};

[[nodiscard]] CommandLineParseResult parse_command_line(
    int argc,
    const char* const* argv) noexcept;

[[nodiscard]] constexpr std::string_view to_string(
    const CommandLineErrorCode code) noexcept {
    switch (code) {
        case CommandLineErrorCode::none:
            return "none";
        case CommandLineErrorCode::allocation_failure:
            return "allocation_failure";
        case CommandLineErrorCode::unknown_argument:
            return "unknown_argument";
        case CommandLineErrorCode::missing_value:
            return "missing_value";
        case CommandLineErrorCode::duplicate_option:
            return "duplicate_option";
        case CommandLineErrorCode::invalid_venue:
            return "invalid_venue";
        case CommandLineErrorCode::invalid_symbols:
            return "invalid_symbols";
        case CommandLineErrorCode::invalid_duration:
            return "invalid_duration";
        case CommandLineErrorCode::invalid_output_directory:
            return "invalid_output_directory";
        case CommandLineErrorCode::missing_replay_output:
            return "missing_replay_output";
        case CommandLineErrorCode::missing_mode:
            return "missing_mode";
        case CommandLineErrorCode::missing_live_option:
            return "missing_live_option";
        case CommandLineErrorCode::mixed_modes:
            return "mixed_modes";
        case CommandLineErrorCode::too_many_replay_inputs:
            return "too_many_replay_inputs";
        case CommandLineErrorCode::duplicate_replay_input:
            return "duplicate_replay_input";
        case CommandLineErrorCode::help_with_other_options:
            return "help_with_other_options";
    }
    return "unknown";
}

}  // namespace hft
