#include "test_framework.h"

#include "hft/command_line.h"

#include <array>
#include <string>

namespace hft::test {
namespace {

template <std::size_t Size>
CommandLineParseResult parse(
    const std::array<const char*, Size>& arguments) {
    return parse_command_line(
        static_cast<int>(arguments.size()), arguments.data());
}

void test_help_and_missing_mode(Context& context) {
    constexpr std::array<const char*, 2U> help{
        "binance_capture", "--help"};
    context.expect(
        parse(help).success() &&
            parse(help).options.mode == ApplicationMode::help,
        "help is accepted as a standalone mode");

    constexpr std::array<const char*, 1U> empty{"binance_capture"};
    context.expect(
        parse(empty).error == CommandLineErrorCode::missing_mode,
        "missing mode is rejected");

    constexpr std::array<const char*, 3U> mixed_help{
        "binance_capture", "--help", "--replay"};
    context.expect(
        parse(mixed_help).error ==
            CommandLineErrorCode::missing_value,
        "missing value is diagnosed before help exclusivity");
}

void test_live_mode(Context& context) {
    constexpr std::array<const char*, 9U> valid{
        "binance_capture",
        "--venue",
        "spot",
        "--symbols",
        "btcusdt,ETHUSDT",
        "--duration",
        "300",
        "--output-dir",
        "capture"};
    const CommandLineParseResult result = parse(valid);
    context.expect(
        result.success() &&
            result.options.mode == ApplicationMode::live_capture &&
            result.options.venue == PayloadVenue::spot &&
            result.options.symbols.size() == 2U &&
            result.options.symbols[0] == "BTCUSDT" &&
            result.options.symbols[1] == "ETHUSDT" &&
            result.options.duration_seconds == 300U &&
            result.options.output_directory == "capture",
        "live options parse and symbols normalize once");

    constexpr std::array<const char*, 3U> missing_symbols{
        "binance_capture", "--venue", "usdm"};
    context.expect(
        parse(missing_symbols).error ==
            CommandLineErrorCode::missing_live_option,
        "live venue requires symbols");

    constexpr std::array<const char*, 5U> duplicate_symbols{
        "binance_capture",
        "--venue",
        "spot",
        "--symbols",
        "BTCUSDT,btcusdt"};
    context.expect(
        parse(duplicate_symbols).error ==
            CommandLineErrorCode::invalid_symbols,
        "symbols duplicated after normalization are rejected");

    constexpr std::array<const char*, 7U> zero_duration{
        "binance_capture",
        "--venue",
        "spot",
        "--symbols",
        "BTCUSDT",
        "--duration",
        "0"};
    context.expect(
        parse(zero_duration).error ==
            CommandLineErrorCode::invalid_duration,
        "zero duration is rejected");

    constexpr std::array<const char*, 7U> excessive_duration{
        "binance_capture",
        "--venue",
        "spot",
        "--symbols",
        "BTCUSDT",
        "--duration",
        "18446744073709551615"};
    context.expect(
        parse(excessive_duration).error ==
            CommandLineErrorCode::invalid_duration,
        "duration exceeding the steady timer range is rejected");

    constexpr std::array<const char*, 5U> invalid_symbol{
        "binance_capture",
        "--venue",
        "spot",
        "--symbols",
        "BTC-USDT"};
    context.expect(
        parse(invalid_symbol).error ==
            CommandLineErrorCode::invalid_symbols,
        "symbol metacharacters are rejected");

    constexpr std::array<const char*, 5U> duplicate_venue{
        "binance_capture",
        "--venue",
        "spot",
        "--venue",
        "usdm"};
    context.expect(
        parse(duplicate_venue).error ==
            CommandLineErrorCode::duplicate_option,
        "single-valued options cannot be repeated");
}

void test_replay_mode(Context& context) {
    constexpr std::array<const char*, 7U> valid{
        "binance_capture",
        "--replay",
        "spot.csv",
        "--replay",
        "usdm.csv",
        "--output-dir",
        "replay"};
    const CommandLineParseResult result = parse(valid);
    context.expect(
        result.success() &&
            result.options.mode == ApplicationMode::replay &&
            result.options.replay_inputs.size() == 2U,
        "repeated replay options are accepted");

    constexpr std::array<const char*, 5U> mixed{
        "binance_capture",
        "--replay",
        "spot.csv",
        "--venue",
        "spot"};
    context.expect(
        parse(mixed).error == CommandLineErrorCode::mixed_modes,
        "replay and live options are mutually exclusive");

    constexpr std::array<const char*, 5U> duplicate{
        "binance_capture",
        "--replay",
        "spot.csv",
        "--replay",
        "spot.csv"};
    context.expect(
        parse(duplicate).error ==
            CommandLineErrorCode::duplicate_replay_input,
        "textually duplicate replay inputs are rejected");

    constexpr std::array<const char*, 3U> missing_output{
        "binance_capture", "--replay", "spot.csv"};
    context.expect(
        parse(missing_output).error ==
            CommandLineErrorCode::missing_replay_output,
        "replay requires an explicit distinct output directory");

    constexpr std::array<const char*, 4U> exclusive_help{
        "binance_capture", "--help", "--output-dir", "unused"};
    context.expect(
        parse(exclusive_help).error ==
            CommandLineErrorCode::help_with_other_options,
        "help cannot be combined with operational options");
}

void test_unsupported_surface(Context& context) {
    constexpr std::array<const char*, 3U> base_url{
        "binance_capture", "--base-url", "wss://example.invalid"};
    context.expect(
        parse(base_url).error ==
            CommandLineErrorCode::unknown_argument,
        "production endpoint overrides are not exposed");

    constexpr std::array<const char*, 2U> insecure{
        "binance_capture", "--insecure"};
    context.expect(
        parse(insecure).error ==
            CommandLineErrorCode::unknown_argument,
        "TLS verification bypass is not exposed");
}

}  // namespace

void run_command_line_tests(Context& context) {
    test_help_and_missing_mode(context);
    test_live_mode(context);
    test_replay_mode(context);
    test_unsupported_surface(context);
}

}  // namespace hft::test
