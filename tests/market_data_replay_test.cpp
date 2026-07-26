#include "test_framework.h"

#include "hft/market_data_replay.h"

#include <array>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>

#include <unistd.h>

namespace hft::test {
namespace {

class TemporaryDirectory {
public:
    TemporaryDirectory() {
        static unsigned sequence{0U};
        path_ = std::filesystem::temp_directory_path() /
            ("hft_replay_" + std::to_string(::getpid()) + "_" +
             std::to_string(sequence++));
        std::filesystem::create_directory(path_);
    }

    ~TemporaryDirectory() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    [[nodiscard]] const std::filesystem::path& path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_{};
};

void write_binary_file(
    const std::filesystem::path& path,
    const std::string_view bytes) {
    std::ofstream output{path, std::ios::binary};
    output.write(
        bytes.data(),
        static_cast<std::streamsize>(bytes.size()));
}

[[nodiscard]] std::string read_binary_file(
    const std::filesystem::path& path) {
    std::ifstream input{path, std::ios::binary};
    return std::string{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}};
}

[[nodiscard]] std::string audit_row(
    const std::string_view symbol,
    const PayloadVenue venue,
    const SpotStreamKind stream_kind,
    const std::uint64_t epoch,
    const std::uint64_t sequence,
    const std::string_view payload,
    const std::uint64_t seconds = 1'700'000'000U,
    const std::uint32_t nanoseconds = 123U) {
    const MarketDataCsvRow row{
        CsvTimestamp{seconds, nanoseconds},
        venue,
        stream_kind,
        0U,
        epoch,
        sequence,
        symbol,
        payload,
    };
    CsvRecordBuffer buffer;
    if (format_market_data_csv_row(row, buffer) !=
        CsvFormatError::none) {
        return {};
    }
    return std::string{buffer.view()};
}

[[nodiscard]] std::filesystem::path source_fixture(
    const std::string_view relative_path) {
    return std::filesystem::path{HFT_SOURCE_DIR} /
        std::filesystem::path{relative_path};
}

void expect_next_error(
    Context& context,
    const std::string_view data_bytes,
    const ReplayReadErrorCode expected_code,
    const ReplayColumn expected_column,
    const std::string_view description,
    const std::uint64_t expected_record_number = 2U) {
    TemporaryDirectory directory;
    const auto path = directory.path() / "input.csv";
    write_binary_file(
        path, std::string{kMarketDataCsvHeader} +
                  std::string{data_bytes});

    ReplayReadError open_error;
    auto reader =
        MarketDataReplayReader::open(path.string(), open_error);
    context.expect(
        reader != nullptr && !open_error,
        std::string{description} + " opens before row validation");
    if (!reader) {
        return;
    }
    ReplayReadResult result;
    do {
        result = reader->next();
    } while (result.status == ReplayReadStatus::record);
    context.expect(
        result.status == ReplayReadStatus::error &&
            result.error.code == expected_code &&
            result.error.column == expected_column &&
            result.error.logical_record_number ==
                expected_record_number,
        std::string{description} +
            " reports stable row and column context");
}

void test_rfc4180_reader_and_padded_reuse(Context& context) {
    TemporaryDirectory directory;
    const auto path = directory.path() / "quoted.csv";
    const std::string first_payload =
        "{\"note\":\"line1\r\nline2\",\"quoted\":\"x\\\"y\"}";
    const std::string second_payload = "{\"value\":2}";
    const std::string bytes =
        std::string{kMarketDataCsvHeader} +
        audit_row(
            "BTCUSDT",
            PayloadVenue::spot,
            SpotStreamKind::trade,
            0U,
            1U,
            first_payload) +
        audit_row(
            "BTCUSDT",
            PayloadVenue::spot,
            SpotStreamKind::trade,
            0U,
            2U,
            second_payload);
    write_binary_file(path, bytes);

    ReplayReadError open_error;
    auto reader =
        MarketDataReplayReader::open(path.string(), open_error);
    context.expect(
        reader != nullptr && !open_error,
        "reader accepts the exact market-data header");
    if (!reader) {
        return;
    }

    ReplayReadResult result = reader->next();
    context.expect(
        result.status == ReplayReadStatus::record &&
            reader->logical_record_number() == 2U &&
            reader->record().context().connection_sequence == 1U &&
            reader->record().symbol() == "BTCUSDT" &&
            std::string_view{
                reader->record().payload().data,
                reader->record().payload().size} == first_payload,
        "quoted commas, doubled quotes, and embedded CRLF decode exactly");
    const char* const reused_payload_address =
        reader->record().payload().data;

    result = reader->next();
    context.expect(
        result.status == ReplayReadStatus::record &&
            reader->logical_record_number() == 3U &&
            reader->record().payload().data ==
                reused_payload_address &&
            std::string_view{
                reader->record().payload().data,
                reader->record().payload().size} == second_payload,
        "next row reuses the same padded payload allocation");
    result = reader->next();
    context.expect(
        result.status == ReplayReadStatus::end_of_file,
        "complete LF-terminated input reaches clean EOF");
}

void test_header_and_column_validation(Context& context) {
    {
        TemporaryDirectory directory;
        const auto path = directory.path() / "header.csv";
        write_binary_file(path, "wrong,header\n");
        ReplayReadError error;
        auto reader =
            MarketDataReplayReader::open(path.string(), error);
        context.expect(
            !reader &&
                error.code == ReplayReadErrorCode::invalid_header &&
                error.column == ReplayColumn::header &&
                error.logical_record_number == 1U,
            "wrong header fails before a data row is exposed");
    }

    expect_next_error(
        context,
        "1,2,spot\n",
        ReplayReadErrorCode::wrong_column_count,
        ReplayColumn::venue,
        "short row");
    expect_next_error(
        context,
        "x,2,spot,trade,0,0,1,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_integer,
        ReplayColumn::recv_tsec,
        "nonnumeric timestamp");
    expect_next_error(
        context,
        "1,1000000000,spot,trade,0,0,1,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_range,
        ReplayColumn::recv_tnsec,
        "nanosecond overflow");
    expect_next_error(
        context,
        "1,2,spot,trade,1,0,1,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_range,
        ReplayColumn::shard_id,
        "nonbaseline shard");
    expect_next_error(
        context,
        "1,2,other,trade,0,0,1,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_enum,
        ReplayColumn::venue,
        "unknown venue");
    expect_next_error(
        context,
        "1,2,spot,other,0,0,1,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_enum,
        ReplayColumn::stream_kind,
        "unknown stream kind");
    expect_next_error(
        context,
        "1,2,spot,trade,0,0,0,BTCUSDT,\"{}\"\n",
        ReplayReadErrorCode::invalid_range,
        ReplayColumn::conn_seq,
        "zero connection sequence");
    expect_next_error(
        context,
        "1,2,spot,trade,0,0,1,btc-usdt,\"{}\"\n",
        ReplayReadErrorCode::invalid_symbol,
        ReplayColumn::symbol,
        "invalid normalized symbol");
    expect_next_error(
        context,
        "1,2,spot,trade,0,0,1,BTCUSDT,{}\n",
        ReplayReadErrorCode::invalid_csv,
        ReplayColumn::payload_json,
        "unquoted payload");
    expect_next_error(
        context,
        "1,2,spot,trade,0,0,1,BTCUSDT,\"{}\"tail\n",
        ReplayReadErrorCode::invalid_csv,
        ReplayColumn::none,
        "bytes after closing quote");
    expect_next_error(
        context,
        "1,2,spot,trade,0,0,1,BTCUSDT,\"{}\"",
        ReplayReadErrorCode::truncated_record,
        ReplayColumn::none,
        "missing final LF");
}

void test_payload_and_record_limits(Context& context) {
    const std::string prefix =
        "1,2,spot,trade,0,0,1,BTCUSDT,\"";
    {
        const std::string payload(kMaxPayloadBytes, 'x');
        TemporaryDirectory directory;
        const auto path = directory.path() / "exact.csv";
        write_binary_file(
            path,
            std::string{kMarketDataCsvHeader} + prefix +
                payload + "\"\n");
        ReplayReadError error;
        auto reader =
            MarketDataReplayReader::open(path.string(), error);
        const ReplayReadResult read =
            reader ? reader->next() : ReplayReadResult{};
        context.expect(
            reader != nullptr &&
                read.status == ReplayReadStatus::record &&
                reader->record().payload().size ==
                    kMaxPayloadBytes,
            "decoded payload accepts the exact 1 MiB limit");
    }
    {
        const std::string payload(kMaxPayloadBytes + 1U, 'x');
        expect_next_error(
            context,
            prefix + payload + "\"\n",
            ReplayReadErrorCode::payload_too_large,
            ReplayColumn::payload_json,
            "decoded payload above 1 MiB");
    }
    {
        const std::string oversized(kMaxCsvRecordBytes, 'x');
        expect_next_error(
            context,
            prefix + oversized + "\"\n",
            ReplayReadErrorCode::record_too_large,
            ReplayColumn::none,
            "logical record above 3 MiB");
    }
}

void test_file_identity_and_sequence_validation(Context& context) {
    const std::string first = audit_row(
        "BTCUSDT",
        PayloadVenue::spot,
        SpotStreamKind::trade,
        2U,
        10U,
        "{}");

    expect_next_error(
        context,
        first + audit_row(
                    "ETHUSDT",
                    PayloadVenue::spot,
                    SpotStreamKind::trade,
                    2U,
                    11U,
                    "{}"),
        ReplayReadErrorCode::changing_symbol,
        ReplayColumn::symbol,
        "symbol change",
        3U);
    expect_next_error(
        context,
        first + audit_row(
                    "BTCUSDT",
                    PayloadVenue::usdm,
                    SpotStreamKind::trade,
                    2U,
                    11U,
                    "{}"),
        ReplayReadErrorCode::changing_venue,
        ReplayColumn::venue,
        "venue change",
        3U);
    expect_next_error(
        context,
        first + audit_row(
                    "BTCUSDT",
                    PayloadVenue::spot,
                    SpotStreamKind::trade,
                    1U,
                    11U,
                    "{}"),
        ReplayReadErrorCode::decreasing_epoch,
        ReplayColumn::conn_epoch,
        "decreasing epoch",
        3U);
    expect_next_error(
        context,
        first + audit_row(
                    "BTCUSDT",
                    PayloadVenue::spot,
                    SpotStreamKind::trade,
                    2U,
                    10U,
                    "{}"),
        ReplayReadErrorCode::
            non_increasing_connection_sequence,
        ReplayColumn::conn_seq,
        "non-increasing same-epoch sequence",
        3U);

    TemporaryDirectory directory;
    const auto path = directory.path() / "epoch-jump.csv";
    write_binary_file(
        path,
        std::string{kMarketDataCsvHeader} + first +
            audit_row(
                "BTCUSDT",
                PayloadVenue::spot,
                SpotStreamKind::trade,
                5U,
                1U,
                "{}"));
    ReplayReadError error;
    auto reader =
        MarketDataReplayReader::open(path.string(), error);
    const ReplayReadResult first_read =
        reader ? reader->next() : ReplayReadResult{};
    const ReplayReadResult second_read =
        reader ? reader->next() : ReplayReadResult{};
    context.expect(
        reader != nullptr &&
            first_read.status == ReplayReadStatus::record &&
            second_read.status == ReplayReadStatus::record,
        "epoch may jump and conn_seq may restart at one");
}

void test_spot_and_usdm_fixture_equivalence(Context& context) {
    TemporaryDirectory directory;
    const auto output_directory = directory.path() / "output";
    constexpr std::array<ReplayOutputSpec, 2U> specifications{{
        ReplayOutputSpec{
            "BTCUSDT",
            "market_data_spot_BTCUSDT_fixture"},
        ReplayOutputSpec{
            "ETHUSDT",
            "market_data_usdm_ETHUSDT_fixture"},
    }};
    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_replay(
        output_directory.string(),
        specifications.data(),
        specifications.size(),
        open_error);
    if (!output) {
        context.expect(false, "fixture output set opens");
        return;
    }

    const auto spot_input = source_fixture(
        "testdata/replay/market_data_spot_BTCUSDT_fixture.csv");
    const auto usdm_input = source_fixture(
        "testdata/replay/market_data_usdm_ETHUSDT_fixture.csv");
    const ReplayFileResult spot = replay_market_data_file(
        spot_input.string(), *output, 0U);
    const ReplayFileResult usdm = replay_market_data_file(
        usdm_input.string(), *output, 1U);
    const OutputSetWriteError close_error = output->close_all();
    context.expect(
        spot.success() && spot.rows_read == 6U &&
            spot.rows_processed == 6U &&
            spot.order_book_rows == 3U &&
            usdm.success() && usdm.rows_read == 6U &&
            usdm.rows_processed == 6U &&
            usdm.order_book_rows == 4U &&
            !close_error,
        "Spot and USD-M fixtures replay through shared production semantics");

    const auto actual_spot = output_directory /
        "market_data_spot_BTCUSDT_fixture_orderbook.csv";
    const auto expected_spot = source_fixture(
        "testdata/replay/expected/"
        "market_data_spot_BTCUSDT_fixture_orderbook.csv");
    const auto actual_usdm = output_directory /
        "market_data_usdm_ETHUSDT_fixture_orderbook.csv";
    const auto expected_usdm = source_fixture(
        "testdata/replay/expected/"
        "market_data_usdm_ETHUSDT_fixture_orderbook.csv");
    context.expect(
        read_binary_file(actual_spot) ==
                read_binary_file(expected_spot) &&
            read_binary_file(actual_usdm) ==
                read_binary_file(expected_usdm),
        "regenerated fixture outputs are byte-identical");
}

void test_runner_rejects_corrupt_payload_and_wrong_target(
    Context& context) {
    TemporaryDirectory directory;
    const auto corrupt_input = directory.path() / "corrupt.csv";
    write_binary_file(
        corrupt_input,
        std::string{kMarketDataCsvHeader} +
            audit_row(
                "BTCUSDT",
                PayloadVenue::spot,
                SpotStreamKind::depth_diff,
                0U,
                1U,
                R"({"e":"depthUpdate")"));

    constexpr std::array<ReplayOutputSpec, 1U> specification{{
        ReplayOutputSpec{"BTCUSDT", "corrupt"},
    }};
    OutputSetOpenError open_error;
    auto output = CsvOutputSet::open_replay(
        (directory.path() / "corrupt-output").string(),
        specification.data(),
        specification.size(),
        open_error);
    if (!output) {
        context.expect(false, "corrupt-payload output opens");
        return;
    }
    const ReplayFileResult corrupt = replay_market_data_file(
        corrupt_input.string(), *output, 0U);
    context.expect(
        corrupt.error == ReplayFileErrorCode::payload_rejected &&
            corrupt.logical_record_number == 2U &&
            corrupt.process_error.parse_error ==
                SpotParseError::malformed_json &&
            corrupt.order_book_rows == 0U,
        "syntactically impossible audit payload fails replay at its row");
    static_cast<void>(output->close_all());

    TemporaryDirectory mismatch_directory;
    constexpr std::array<ReplayOutputSpec, 1U> mismatch_spec{{
        ReplayOutputSpec{"ETHUSDT", "mismatch"},
    }};
    OutputSetOpenError mismatch_open_error;
    auto mismatch_output = CsvOutputSet::open_replay(
        (mismatch_directory.path() / "output").string(),
        mismatch_spec.data(),
        mismatch_spec.size(),
        mismatch_open_error);
    if (!mismatch_output) {
        context.expect(false, "target-mismatch output opens");
        return;
    }
    const ReplayFileResult mismatch = replay_market_data_file(
        source_fixture(
            "testdata/replay/"
            "market_data_spot_BTCUSDT_fixture.csv")
            .string(),
        *mismatch_output,
        0U);
    context.expect(
        mismatch.error ==
                ReplayFileErrorCode::output_target_mismatch &&
            mismatch.logical_record_number == 2U,
        "input symbol cannot be routed to another output target");
    static_cast<void>(mismatch_output->close_all());
}

}  // namespace

void run_market_data_replay_tests(Context& context) {
    test_rfc4180_reader_and_padded_reuse(context);
    test_header_and_column_validation(context);
    test_payload_and_record_limits(context);
    test_file_identity_and_sequence_validation(context);
    test_spot_and_usdm_fixture_equivalence(context);
    test_runner_rejects_corrupt_payload_and_wrong_target(context);
}

}  // namespace hft::test
