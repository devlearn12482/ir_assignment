#include "hft/event_processor.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr std::size_t kDefaultIterations{1'000'000U};
constexpr std::size_t kWarmupIterations{10'000U};
constexpr std::string_view kPayload{
    R"({"lastUpdateId":100,"bids":[["100.50","1.25"],["100.40","2.50"],["100.30","3.75"],["100.20","4.00"],["100.10","5.50"]],"asks":[["100.60","1.50"],["100.70","2.75"],["100.80","3.00"],["100.90","4.25"],["101.00","5.75"]]})"};

[[nodiscard]] bool parse_iterations(
    const int argc,
    const char* const* const argv,
    std::size_t& iterations) noexcept {
    if (argc == 1) {
        iterations = kDefaultIterations;
        return true;
    }
    if (argc != 2 || argv[1] == nullptr) {
        return false;
    }
    const std::string_view value{argv[1]};
    std::uint64_t parsed = 0U;
    const std::from_chars_result result = std::from_chars(
        value.data(), value.data() + value.size(), parsed);
    if (result.ec != std::errc{} ||
        result.ptr != value.data() + value.size() || parsed == 0U ||
        parsed > static_cast<std::uint64_t>(
                     std::numeric_limits<std::size_t>::max())) {
        return false;
    }
    iterations = static_cast<std::size_t>(parsed);
    return true;
}

[[nodiscard]] std::uint64_t percentile(
    const std::vector<std::uint64_t>& sorted,
    const std::size_t numerator,
    const std::size_t denominator) noexcept {
    const std::size_t index =
        (sorted.size() - 1U) * numerator / denominator;
    return sorted[index];
}

[[nodiscard]] bool process_one(
    hft::EventProcessor& processor,
    hft::SymbolState& state,
    hft::EventRowBatch& rows,
    const hft::PaddedJsonView payload,
    const std::uint64_t connection_sequence,
    std::uint64_t& checksum) noexcept {
    const hft::EventContext context{
        hft::CsvTimestamp{1'700'000'000U, 123'456'789U},
        hft::PayloadVenue::spot,
        hft::SpotStreamKind::depth5,
        0U,
        0U,
        connection_sequence,
        "BTCUSDT"};
    const hft::EventProcessResult result = processor.process(
        state,
        context,
        payload,
        hft::EventOutputMode::live_capture,
        rows);
    if (!result.success() ||
        result.status != hft::EventProcessStatus::applied_refresh ||
        !rows.has_audit_row || !rows.has_order_book_row) {
        return false;
    }
    checksum += static_cast<std::uint64_t>(rows.audit_row.size());
    checksum += static_cast<std::uint64_t>(
        rows.order_book_row.size());
    return true;
}

}  // namespace

int main(const int argc, const char* const* const argv) {
    std::size_t iterations = 0U;
    if (!parse_iterations(argc, argv, iterations)) {
        std::cerr << "usage: hft_core_benchmark [positive-iterations]\n";
        return 2;
    }

    std::string padded_payload{kPayload};
    padded_payload.resize(
        kPayload.size() + hft::kJsonPaddingBytes, '\0');
    const hft::PaddedJsonView payload{
        padded_payload.data(), kPayload.size(), padded_payload.size()};

    auto state = hft::SymbolState::create(
        hft::PayloadVenue::spot, "BTCUSDT");
    if (!state) {
        std::cerr << "benchmark state initialization failed\n";
        return 3;
    }

    hft::SymbolState& symbol_state = *state;
    hft::EventProcessor processor;
    hft::EventRowBatch rows;
    std::uint64_t checksum = 0U;
    std::uint64_t connection_sequence = 1U;

    for (std::size_t index = 0U; index < kWarmupIterations; ++index) {
        if (!process_one(
                processor,
                symbol_state,
                rows,
                payload,
                connection_sequence++,
                checksum)) {
            std::cerr << "benchmark warmup failed\n";
            return 4;
        }
    }

    std::vector<std::uint64_t> latencies(iterations);
    const auto run_start = std::chrono::steady_clock::now();
    for (std::size_t index = 0U; index < iterations; ++index) {
        const auto event_start = std::chrono::steady_clock::now();
        if (!process_one(
                processor,
                symbol_state,
                rows,
                payload,
                connection_sequence++,
                checksum)) {
            std::cerr << "benchmark event failed at index=" << index
                      << '\n';
            return 5;
        }
        const auto event_end = std::chrono::steady_clock::now();
        latencies[index] = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                event_end - event_start)
                .count());
    }
    const auto run_end = std::chrono::steady_clock::now();

    std::sort(latencies.begin(), latencies.end());
    const std::uint64_t elapsed_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            run_end - run_start)
            .count());
    const double elapsed_seconds =
        static_cast<double>(elapsed_ns) / 1'000'000'000.0;
    const double events_per_second =
        static_cast<double>(iterations) / elapsed_seconds;

    std::cout << "benchmark=event_processor_spot_depth5\n"
              << "iterations=" << iterations << '\n'
              << "warmup_iterations=" << kWarmupIterations << '\n'
              << "payload_bytes=" << kPayload.size() << '\n'
              << "elapsed_ns=" << elapsed_ns << '\n'
              << "events_per_second="
              << static_cast<std::uint64_t>(events_per_second) << '\n'
              << "latency_p50_ns="
              << percentile(latencies, 50U, 100U) << '\n'
              << "latency_p95_ns="
              << percentile(latencies, 95U, 100U) << '\n'
              << "latency_p99_ns="
              << percentile(latencies, 99U, 100U) << '\n'
              << "checksum=" << checksum << '\n';
    return 0;
}
