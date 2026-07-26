#include "hft/command_line.h"

#include "hft/csv_output_set.h"
#include "hft/market_data_replay.h"
#include "hft/symbol_identity.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace {

constexpr int kExitSuccess{0};
constexpr int kExitCommandLine{2};
constexpr int kExitInput{3};
constexpr int kExitOutput{4};
constexpr int kExitProcessing{5};
constexpr int kExitInternal{6};

struct ReplayInputMetadata {
    std::string path{};
    std::string normalized_path{};
    std::string normalized_parent{};
    std::string symbol{};
    std::string stem{};
};

void print_usage(std::ostream& output) {
    output
        << "Usage:\n"
        << "  binance_capture --venue <spot|usdm> "
           "--symbols <SYMBOL[,SYMBOL...]> "
           "[--duration <seconds>] [--output-dir <directory>]\n"
        << "  binance_capture --replay <market_data.csv> "
           "[--replay <market_data.csv>...] "
           "--output-dir <distinct-directory>\n"
        << "  binance_capture --help\n";
}

[[nodiscard]] std::string normalized_weakly_canonical_path(
    const std::filesystem::path& path,
    std::error_code& error) {
    const std::filesystem::path absolute =
        std::filesystem::weakly_canonical(path, error);
    if (error) {
        return {};
    }
    return absolute.lexically_normal().string();
}

[[nodiscard]] bool inspect_replay_input(
    const std::string& path,
    ReplayInputMetadata& metadata) {
    namespace filesystem = std::filesystem;
    std::error_code error;
    const filesystem::path input_path{path};
    if (!filesystem::is_regular_file(input_path, error) || error) {
        std::cerr << "error category=input_not_regular path=\""
                  << path << "\" native_error=" << error.value()
                  << '\n';
        return false;
    }

    metadata.path = path;
    metadata.normalized_path =
        normalized_weakly_canonical_path(input_path, error);
    if (error) {
        std::cerr << "error category=path_resolution_failed path=\""
                  << path << "\" native_error=" << error.value()
                  << '\n';
        return false;
    }
    metadata.normalized_parent =
        filesystem::path{metadata.normalized_path}
            .parent_path()
            .string();
    metadata.stem = input_path.stem().string();
    if (metadata.stem.empty()) {
        std::cerr << "error category=invalid_input_stem path=\""
                  << path << "\"\n";
        return false;
    }

    hft::ReplayReadError open_error;
    std::unique_ptr<hft::MarketDataReplayReader> reader =
        hft::MarketDataReplayReader::open(path, open_error);
    if (!reader) {
        std::cerr << "error category=" << hft::to_string(open_error.code)
                  << " path=\"" << path << "\" record="
                  << open_error.logical_record_number << " column="
                  << hft::to_string(open_error.column)
                  << " native_error=" << open_error.native_error << '\n';
        return false;
    }
    const hft::ReplayReadResult first = reader->next();
    if (first.status != hft::ReplayReadStatus::record) {
        if (first.status == hft::ReplayReadStatus::error) {
            std::cerr << "error category="
                      << hft::to_string(first.error.code) << " path=\""
                      << path << "\" record="
                      << first.error.logical_record_number << " column="
                      << hft::to_string(first.error.column)
                      << " native_error=" << first.error.native_error
                      << '\n';
        } else {
            std::cerr << "error category=empty_input path=\""
                      << path << "\" record=2 column=none\n";
        }
        return false;
    }
    metadata.symbol = reader->record().symbol();
    return true;
}

void report_output_open_error(
    const hft::OutputSetOpenError& error) {
    std::cerr << "error category=" << hft::to_string(error.code)
              << " path=\"" << error.path << "\" target="
              << error.target_index << " symbol_error="
              << hft::to_string(error.symbol_error)
              << " native_error=" << error.native_error << '\n';
}

void report_replay_error(
    const std::string& path,
    const hft::ReplayFileResult& result) {
    std::cerr << "error category=" << hft::to_string(result.error)
              << " path=\"" << path << "\" record="
              << result.logical_record_number;
    if (result.error == hft::ReplayFileErrorCode::input_error) {
        std::cerr << " input_category="
                  << hft::to_string(result.input_error.code)
                  << " column="
                  << hft::to_string(result.input_error.column)
                  << " native_error="
                  << result.input_error.native_error;
    } else if (
        result.error == hft::ReplayFileErrorCode::event_process_failed ||
        result.error == hft::ReplayFileErrorCode::payload_rejected) {
        std::cerr << " column=payload_json process_status="
                  << hft::to_string(result.process_error.status)
                  << " process_error="
                  << hft::to_string(result.process_error.error)
                  << " parse_error="
                  << hft::to_string(result.process_error.parse_error)
                  << " parse_field="
                  << hft::to_string(result.process_error.parse_field);
    } else if (
        result.error ==
        hft::ReplayFileErrorCode::output_write_failed) {
        std::cerr << " output_category="
                  << hft::to_string(result.output_error.code)
                  << " target=" << result.output_error.target_index
                  << " file_category="
                  << hft::to_string(
                         result.output_error.file_error.code)
                  << " native_error="
                  << result.output_error.file_error.native_error;
    }
    std::cerr << '\n';
}

[[nodiscard]] int run_replay(
    const hft::CommandLineOptions& options) {
    namespace filesystem = std::filesystem;
    std::vector<ReplayInputMetadata> metadata(
        options.replay_inputs.size());
    for (std::size_t index = 0;
         index < options.replay_inputs.size();
         ++index) {
        if (!inspect_replay_input(
                options.replay_inputs[index], metadata[index])) {
            return kExitInput;
        }
        for (std::size_t previous = 0; previous < index; ++previous) {
            if (metadata[index].normalized_path ==
                metadata[previous].normalized_path) {
                std::cerr
                    << "error category=duplicate_replay_input path=\""
                    << metadata[index].path << "\"\n";
                return kExitCommandLine;
            }
            if (metadata[index].symbol == metadata[previous].symbol) {
                std::cerr
                    << "error category=duplicate_replay_symbol path=\""
                    << metadata[index].path << "\" symbol="
                    << metadata[index].symbol << '\n';
                return kExitInput;
            }
            if (metadata[index].stem == metadata[previous].stem) {
                std::cerr
                    << "error category=duplicate_output_stem path=\""
                    << metadata[index].path << "\" stem="
                    << metadata[index].stem << '\n';
                return kExitInput;
            }
        }
    }

    std::error_code path_error;
    const std::string output_path = normalized_weakly_canonical_path(
        filesystem::path{options.output_directory}, path_error);
    if (path_error) {
        std::cerr << "error category=path_resolution_failed path=\""
                  << options.output_directory << "\" native_error="
                  << path_error.value() << '\n';
        return kExitOutput;
    }
    for (const ReplayInputMetadata& input : metadata) {
        if (output_path == input.normalized_parent) {
            std::cerr
                << "error category=output_matches_input_directory path=\""
                << options.output_directory << "\" input=\""
                << input.path << "\"\n";
            return kExitOutput;
        }
    }

    std::vector<hft::ReplayOutputSpec> specifications;
    specifications.reserve(metadata.size());
    for (const ReplayInputMetadata& input : metadata) {
        specifications.push_back(
            hft::ReplayOutputSpec{input.symbol, input.stem});
    }
    hft::OutputSetOpenError open_error;
    std::unique_ptr<hft::CsvOutputSet> output =
        hft::CsvOutputSet::open_replay(
            options.output_directory,
            specifications.data(),
            specifications.size(),
            open_error);
    if (!output) {
        report_output_open_error(open_error);
        return kExitOutput;
    }

    std::uint64_t rows_read = 0;
    std::uint64_t rows_processed = 0;
    std::uint64_t book_rows = 0;
    int exit_code = kExitSuccess;
    for (std::size_t index = 0; index < metadata.size(); ++index) {
        const hft::ReplayFileResult replay =
            hft::replay_market_data_file(
                metadata[index].path, *output, index);
        rows_read += replay.rows_read;
        rows_processed += replay.rows_processed;
        book_rows += replay.order_book_rows;
        if (!replay.success()) {
            report_replay_error(metadata[index].path, replay);
            exit_code = replay.error ==
                    hft::ReplayFileErrorCode::input_error ||
                replay.error == hft::ReplayFileErrorCode::empty_input
                ? kExitInput
                : kExitProcessing;
            break;
        }
    }

    const hft::OutputSetWriteError close_error =
        output->close_all();
    if (close_error) {
        std::cerr << "error category=output_close_failed target="
                  << close_error.target_index << " file_kind="
                  << static_cast<unsigned>(close_error.file_kind)
                  << " file_category="
                  << hft::to_string(close_error.file_error.code)
                  << " native_error="
                  << close_error.file_error.native_error << '\n';
        exit_code = kExitOutput;
    }
    if (exit_code == kExitSuccess) {
        std::cout << "replay_complete files=" << metadata.size()
                  << " rows_read=" << rows_read
                  << " rows_processed=" << rows_processed
                  << " order_book_rows=" << book_rows
                  << " output_dir=\"" << options.output_directory
                  << "\"\n";
    }
    return exit_code;
}

[[nodiscard]] int run_application(
    const int argc,
    const char* const* const argv) {
    const hft::CommandLineParseResult command_line =
        hft::parse_command_line(argc, argv);
    if (!command_line.success()) {
        std::cerr << "error category="
                  << hft::to_string(command_line.error)
                  << " argument_index="
                  << command_line.argument_index << '\n';
        print_usage(std::cerr);
        return kExitCommandLine;
    }
    if (command_line.options.mode == hft::ApplicationMode::help) {
        print_usage(std::cout);
        return kExitSuccess;
    }
    if (command_line.options.mode ==
        hft::ApplicationMode::live_capture) {
        std::cerr
            << "error category=live_capture_unavailable "
               "detail=\"live networking is not implemented in this "
               "increment\"\n";
        return kExitInternal;
    }
    return run_replay(command_line.options);
}

}  // namespace

int main(const int argc, const char* const* const argv) {
    try {
        return run_application(argc, argv);
    } catch (const std::bad_alloc&) {
        std::cerr << "error category=allocation_failure\n";
        return kExitInternal;
    } catch (const std::exception& error) {
        std::cerr << "error category=unexpected_exception detail=\""
                  << error.what() << "\"\n";
        return kExitInternal;
    } catch (...) {
        std::cerr << "error category=unknown_exception\n";
        return kExitInternal;
    }
}
