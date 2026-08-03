#include "hft/command_line.h"

#include "hft/csv_output_set.h"
#include "hft/live_capture_controller.h"
#include "hft/live_run_loop.h"
#include "hft/live_subscription.h"
#include "hft/market_data_replay.h"
#include "hft/symbol_identity.h"

#include <boost/asio/io_context.hpp>

#include <array>
#include <cerrno>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <ctime>
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

[[nodiscard]] bool current_utc_date(std::string& output) {
    const std::time_t now = std::time(nullptr);
    std::tm utc{};
    if (now == static_cast<std::time_t>(-1) ||
        ::gmtime_r(&now, &utc) == nullptr) {
        return false;
    }
    const int year = utc.tm_year + 1900;
    if (year < 0 || year > 9999) {
        return false;
    }
    std::array<char, 11U> buffer{};
    const int written = std::snprintf(
        buffer.data(),
        buffer.size(),
        "%04d-%02d-%02d",
        year,
        utc.tm_mon + 1,
        utc.tm_mday);
    if (written != 10) {
        return false;
    }
    output.assign(buffer.data(), 10U);
    return true;
}

void report_live_create_error(
    const hft::LiveCaptureCreateError& error) {
    std::cerr << "error category=" << hft::to_string(error.code)
              << " pipeline_category="
              << hft::to_string(error.pipeline_error)
              << " writer_category="
              << hft::to_string(error.writer_error)
              << " session_category="
              << hft::to_string(error.session_error.code)
              << " session_stage="
              << hft::to_string(error.session_error.stage)
              << " native_error="
              << error.session_error.native_error.value() << '\n';
}

[[nodiscard]] int live_exit_code(
    const hft::LiveRunLoopResult& run) noexcept {
    const hft::LiveCaptureResult& result = run.capture;
    if (run.error == hft::LiveRunLoopErrorCode::none &&
        result.success()) {
        return kExitSuccess;
    }
    if (result.error == hft::LiveCaptureErrorCode::writer_failure ||
        result.writer.output_error) {
        return kExitOutput;
    }
    return kExitProcessing;
}

void print_live_metrics(
    const hft::LiveRunLoopResult& run,
    const int exit_code) {
    const hft::LiveCaptureResult& capture = run.capture;
    const hft::CsvWriterMetrics& writer = capture.writer.metrics;
    const hft::WebSocketSessionErrorCode reported_session_error =
        exit_code == kExitSuccess &&
                capture.session.code ==
                    hft::WebSocketSessionErrorCode::cancelled
            ? hft::WebSocketSessionErrorCode::none
            : capture.session.code;
    std::cerr
        << "METRICS_BEGIN version=1\n"
        << "run.mode=live\n"
        << "run.status="
        << (exit_code == kExitSuccess ? "success" : "fatal")
        << "\nrun.exit_code=" << exit_code
        << "\nrun.stop_requested="
        << (capture.stop_requested ? 1 : 0)
        << "\nrun.signals_received=" << run.signals_received
        << "\nrun.repeated_signals=" << run.repeated_signals
        << "\nrun.duration_expired="
        << (run.duration_expired ? 1 : 0)
        << "\nsource.complete_messages="
        << capture.metrics.complete_messages
        << "\nsource.replay_rows_read=0"
        << "\nsource.text_messages="
        << capture.metrics.text_messages
        << "\nconnections.attempts="
        << capture.metrics.connection_attempts
        << "\nconnections.successful="
        << capture.metrics.successful_connections
        << "\nconnections.reconnects_scheduled="
        << capture.metrics.reconnects_scheduled
        << "\nconnections.recoverable_failures="
        << capture.metrics.recoverable_session_failures
        << "\nevents.pre_audit_rejections="
        << capture.metrics.pre_audit_rejections
        << "\nevents.unknown_stream_rejections="
        << capture.metrics.unknown_stream_rejections
        << "\nevents.invalid_data_rejections="
        << capture.metrics.invalid_data_rejections
        << "\nevents.malformed_envelope_rejections="
        << capture.metrics.malformed_envelope_rejections
        << "\nevents.audit_eligible="
        << capture.metrics.audit_eligible_events
        << "\nevents.schema_rejections="
        << capture.metrics.schema_rejections
        << "\nevents.applied_refreshes="
        << capture.metrics.applied_refreshes
        << "\nevents.applied_diffs="
        << capture.metrics.applied_diffs
        << "\nevents.stale_refreshes="
        << capture.metrics.stale_refreshes
        << "\nevents.stale_diffs="
        << capture.metrics.stale_diffs
        << "\nevents.ignored_while_invalid="
        << capture.metrics.ignored_while_invalid
        << "\nevents.sequence_gaps="
        << capture.metrics.sequence_gaps
        << "\nevents.crossed_books="
        << capture.metrics.crossed_books
        << "\nevents.trades_audited="
        << capture.metrics.trades_audited
        << "\nevents.connection_invalidations="
        << capture.metrics.connection_invalidations
        << "\nevents.processed="
        << capture.metrics.processed_events
        << "\nwriter.audit_rows_enqueued="
        << writer.audit_rows_published
        << "\nwriter.audit_rows_written="
        << writer.audit_rows_written
        << "\nwriter.audit_rows_unwritten="
        << writer.audit_rows_unwritten
        << "\nwriter.book_rows_enqueued="
        << writer.order_book_rows_published
        << "\nwriter.book_rows_written="
        << writer.order_book_rows_written
        << "\nwriter.book_rows_unwritten="
        << writer.order_book_rows_unwritten
        << "\npolicy.binary_messages="
        << capture.metrics.binary_messages
        << "\npolicy.oversized_messages="
        << capture.metrics.oversized_messages
        << "\npolicy.breaker_trips="
        << capture.metrics.message_policy_breaker_trips
        << "\nbackpressure.pauses="
        << capture.metrics.producer_pauses
        << "\nbackpressure.resumes="
        << capture.metrics.producer_resumes
        << "\nfailure.capture=" << hft::to_string(capture.error)
        << "\nfailure.control=" << hft::to_string(run.error)
        << "\nfailure.session="
        << hft::to_string(reported_session_error)
        << "\nfailure.writer="
        << hft::to_string(capture.writer.output_error.code)
        << "\nMETRICS_END\n";
}

void print_replay_metrics(
    const std::size_t files,
    const std::uint64_t rows_read,
    const std::uint64_t rows_processed,
    const std::uint64_t book_rows,
    const hft::CsvOutputSetMetrics& output,
    const int exit_code) {
    const std::uint64_t unwritten_book_rows =
        book_rows >= output.order_book_rows_written
            ? book_rows - output.order_book_rows_written
            : 0U;
    std::cerr
        << "METRICS_BEGIN version=1\n"
        << "run.mode=replay\n"
        << "run.status="
        << (exit_code == kExitSuccess ? "success" : "fatal")
        << "\nrun.exit_code=" << exit_code
        << "\nrun.stop_requested=0"
        << "\nsource.complete_messages=0"
        << "\nsource.replay_files=" << files
        << "\nsource.replay_rows_read=" << rows_read
        << "\nconnections.attempts=0"
        << "\nconnections.successful=0"
        << "\nconnections.reconnects_scheduled=0"
        << "\nevents.pre_audit_rejections=0"
        << "\nevents.audit_eligible=0"
        << "\nevents.processed=" << rows_processed
        << "\nwriter.audit_rows_enqueued=0"
        << "\nwriter.audit_rows_written="
        << output.audit_rows_written
        << "\nwriter.audit_rows_unwritten=0"
        << "\nwriter.book_rows_enqueued=" << book_rows
        << "\nwriter.book_rows_written="
        << output.order_book_rows_written
        << "\nwriter.book_rows_unwritten="
        << unwritten_book_rows
        << "\nfailure.replay="
        << (exit_code == kExitSuccess ? "none" : "failed")
        << "\nMETRICS_END\n";
}

[[nodiscard]] int run_live(
    const hft::CommandLineOptions& options) {
    std::array<std::string_view, hft::kMaxConfiguredSymbols> symbols{};
    for (std::size_t index = 0U;
         index < options.symbols.size();
         ++index) {
        symbols[index] = options.symbols[index];
    }
    hft::SubscriptionError subscription_error;
    std::unique_ptr<hft::LiveSubscription> subscription =
        hft::LiveSubscription::create(
            options.venue,
            symbols.data(),
            options.symbols.size(),
            subscription_error);
    if (!subscription) {
        std::cerr << "error category=subscription_initialization_failed"
                  << " detail="
                  << hft::to_string(subscription_error.code)
                  << '\n';
        return kExitInternal;
    }
    hft::VerifiedWebSocketEndpoint endpoint =
        hft::production_websocket_endpoint(*subscription);

    std::string utc_date;
    if (!current_utc_date(utc_date)) {
        std::cerr << "error category=utc_clock_failure\n";
        return kExitInternal;
    }
    hft::OutputSetOpenError output_error;
    std::unique_ptr<hft::CsvOutputSet> output =
        hft::CsvOutputSet::open_live(
            options.output_directory,
            options.venue,
            symbols.data(),
            options.symbols.size(),
            utc_date,
            output_error);
    if (!output) {
        report_output_open_error(output_error);
        return kExitOutput;
    }

    boost::asio::io_context io_context;
    hft::LiveRunLoopCreateError loop_error;
    const std::shared_ptr<hft::LiveRunLoop> loop =
        hft::LiveRunLoop::create(
            io_context, options.duration_seconds, loop_error);
    if (!loop) {
        std::cerr << "error category=" << hft::to_string(loop_error)
                  << '\n';
        return kExitInternal;
    }
    hft::LiveCaptureCreateError capture_error;
    const std::shared_ptr<hft::LiveCaptureController> controller =
        hft::LiveCaptureController::create(
            io_context,
            std::move(subscription),
            std::move(output),
            std::move(endpoint),
            loop->capture_callbacks(),
            capture_error);
    if (!controller) {
        report_live_create_error(capture_error);
        return capture_error.code ==
                    hft::LiveCaptureCreateErrorCode::
                        writer_initialization_failure
                   ? kExitOutput
                   : kExitInternal;
    }
    if (!loop->attach_controller(controller)) {
        std::cerr << "error category=live_loop_attach_failed\n";
        return kExitInternal;
    }

    loop->start();
    io_context.run();
    if (!loop->terminal()) {
        std::cerr << "error category=live_loop_incomplete\n";
        return kExitInternal;
    }
    const int exit_code = live_exit_code(loop->result());
    print_live_metrics(loop->result(), exit_code);
    return exit_code;
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
    print_replay_metrics(
        metadata.size(),
        rows_read,
        rows_processed,
        book_rows,
        output->metrics(),
        exit_code);
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
        return run_live(command_line.options);
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
