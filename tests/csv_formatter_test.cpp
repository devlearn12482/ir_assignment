#include "test_framework.h"

#include "hft/csv_formatter.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

namespace hft::test {
namespace {

static_assert(
    std::is_same_v<decltype(CsvTimestamp::seconds), std::int64_t>);
static_assert(
    std::is_same_v<decltype(CsvTimestamp::nanoseconds), std::int32_t>);

[[nodiscard]] std::size_t comma_count(
    const std::string_view text) noexcept {
    std::size_t count{0};
    for (const char byte : text) {
        if (byte == ',') {
            ++count;
        }
    }
    return count;
}

void test_exact_headers(Context& context) {
    context.expect(
        kMarketDataCsvHeader ==
            "recv_tsec,recv_tnsec,venue,stream_kind,shard_id,"
            "conn_epoch,conn_seq,symbol,payload_json\n",
        "market-data header is byte exact");
    context.expect(
        kOrderBookCsvHeader ==
            "tsec,tnsec,seqNo,id,type,side,bid0,bid1,bid2,bid3,"
            "bid4,bid_size0,bid_size1,bid_size2,bid_size3,"
            "bid_size4,ask0,ask1,ask2,ask3,ask4,ask_size0,"
            "ask_size1,ask_size2,ask_size3,ask_size4\n",
        "order-book header is byte exact");
    context.expect(
        kMarketDataCsvHeader.front() != '\xEF' &&
            kOrderBookCsvHeader.front() != '\xEF',
        "headers have no UTF-8 BOM");
}

void test_market_data_row_escaping(Context& context) {
    CsvRecordBuffer buffer;
    const std::string payload =
        "{\"e\":\"depthUpdate\",\"note\":\"a,b\",\"raw\":\"x\r\ny\"}";
    const MarketDataCsvRow row{
        CsvTimestamp{1'700'000'000, 123'456'789},
        PayloadVenue::spot,
        SpotStreamKind::depth_diff,
        0U,
        2U,
        9U,
        "BTCUSDT",
        payload,
    };

    const CsvFormatError error =
        format_market_data_csv_row(row, buffer);
    const std::string expected =
        "1700000000,123456789,spot,depth_diff,0,2,9,BTCUSDT,"
        "\"{\"\"e\"\":\"\"depthUpdate\"\",\"\"note\"\":\"\"a,b\"\","
        "\"\"raw\"\":\"\"x\r\ny\"\"}\"\n";
    context.expect(
        error == CsvFormatError::none,
        "valid market-data row formats");
    context.expect(
        buffer.view() == expected,
        "payload is always quoted with doubled quotes and preserved bytes");
    context.expect(
        buffer.view().back() == '\n' &&
            buffer.view().substr(buffer.view().size() - 2U) != "\r\n",
        "market-data records use LF terminators");

    MarketDataCsvRow pre_epoch = row;
    pre_epoch.timestamp = CsvTimestamp{-1, 999'999'999};
    context.expect(
        format_market_data_csv_row(pre_epoch, buffer) ==
                CsvFormatError::none &&
            buffer.view().find("-1,999999999,") == 0U,
        "market-data tsec serializes as signed int64");

    MarketDataCsvRow depth5 = row;
    depth5.venue = PayloadVenue::usdm;
    depth5.stream_kind = SpotStreamKind::depth5;
    depth5.payload_json = "{}";
    context.expect(
        format_market_data_csv_row(depth5, buffer) ==
                CsvFormatError::none &&
            buffer.view() ==
                "1700000000,123456789,usdm,depth5,0,2,9,"
                "BTCUSDT,\"{}\"\n",
        "USD-M depth5 enum fields serialize exactly");

    MarketDataCsvRow trade = row;
    trade.stream_kind = SpotStreamKind::trade;
    trade.payload_json = "{}";
    context.expect(
        format_market_data_csv_row(trade, buffer) ==
                CsvFormatError::none &&
            buffer.view().find(",trade,") != std::string_view::npos,
        "trade stream kind serializes exactly");
}

void test_market_data_validation_and_limit(Context& context) {
    CsvRecordBuffer buffer;
    MarketDataCsvRow row{
        CsvTimestamp{1, 2},
        PayloadVenue::spot,
        SpotStreamKind::depth_diff,
        0U,
        0U,
        1U,
        "BTCUSDT",
        "{}",
    };
    context.expect(
        format_market_data_csv_row(row, buffer) ==
            CsvFormatError::none,
        "baseline market-data row is valid");

    row.timestamp.nanoseconds = 1'000'000'000;
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_timestamp &&
            buffer.empty(),
        "invalid nanoseconds fail without a partial row");
    row.timestamp.nanoseconds = -1;
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_timestamp &&
            buffer.empty(),
        "negative nanoseconds fail without a partial row");
    row.timestamp.nanoseconds = 2;

    row.connection_sequence = 0U;
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_sequence &&
            buffer.empty(),
        "zero connection sequence is rejected");
    row.connection_sequence = 1U;

    row.symbol = "BTC,USDT";
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_symbol &&
            buffer.empty(),
        "unsafe symbol is rejected");
    row.symbol = "BTCUSDT";

    row.venue = static_cast<PayloadVenue>(255U);
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_enum &&
            buffer.empty(),
        "invalid venue is rejected");
    row.venue = PayloadVenue::spot;

    row.stream_kind = static_cast<SpotStreamKind>(255U);
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::invalid_enum &&
            buffer.empty(),
        "invalid stream kind is rejected");
    row.stream_kind = SpotStreamKind::depth_diff;

    row.payload_json = "";
    context.expect(
        format_market_data_csv_row(row, buffer) ==
            CsvFormatError::none,
        "empty field establishes boundary-row overhead");
    const std::size_t overhead = buffer.size();
    std::string payload(kMaxCsvRecordBytes - overhead, 'x');
    row.payload_json = payload;
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::none &&
            buffer.size() == kMaxCsvRecordBytes,
        "record exactly at the hard limit is complete");

    payload.push_back('x');
    row.payload_json = payload;
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::record_too_large &&
            buffer.empty(),
        "record one byte above the hard limit is rejected, not cropped");

    row.payload_json = "{}";
    context.expect(
        format_market_data_csv_row(row, buffer) ==
                CsvFormatError::none &&
            buffer.view().substr(buffer.view().size() - 5U) ==
                "\"{}\"\n",
        "record buffer remains reusable after limit rejection");
}

void test_order_book_row(Context& context) {
    constexpr std::array<BookLevel, 2U> bids{{
        BookLevel{10'000'000'000LL, 200'000'000LL},
        BookLevel{9'900'000'000LL, 150'000'000LL},
    }};
    constexpr std::array<BookLevel, 1U> asks{{
        BookLevel{10'100'000'000LL, 300'000'000LL},
    }};
    const OrderBookCsvRow row{
        CsvTimestamp{1'700'000'000, 123'456'789},
        42U,
        123'456'789,
        BookRowType::differential,
        BookRowSide::bid,
        BookSideView{bids.data(), bids.size()},
        BookSideView{asks.data(), asks.size()},
    };

    CsvRecordBuffer buffer;
    const CsvFormatError error =
        format_order_book_csv_row(row, buffer);
    const std::string_view expected =
        "1700000000,123456789,42,123456789,D,B,"
        "10000000000,9900000000,0,0,0,"
        "200000000,150000000,0,0,0,"
        "10100000000,0,0,0,0,"
        "300000000,0,0,0,0\n";
    context.expect(
        error == CsvFormatError::none,
        "valid order-book row formats");
    context.expect(
        buffer.view() == expected,
        "order-book integers and missing levels serialize exactly");
    context.expect(
        comma_count(buffer.view()) == 25U,
        "order-book row contains exactly 26 quote-free columns");
    context.expect(
        buffer.view().find('"') == std::string_view::npos,
        "order-book row is quote-free by construction");

    OrderBookCsvRow pre_epoch = row;
    pre_epoch.timestamp = CsvTimestamp{-1, 999'999'999};
    context.expect(
        format_order_book_csv_row(pre_epoch, buffer) ==
                CsvFormatError::none &&
            buffer.view().find("-1,999999999,") == 0U,
        "order-book tsec serializes as signed int64");

    OrderBookCsvRow refresh = row;
    refresh.row_type = BookRowType::partial_refresh;
    refresh.side = BookRowSide::neutral;
    context.expect(
        format_order_book_csv_row(refresh, buffer) ==
                CsvFormatError::none &&
            buffer.view().find(",P,N,") != std::string_view::npos,
        "partial refresh and neutral side codes serialize exactly");

    OrderBookCsvRow ask_only = row;
    ask_only.side = BookRowSide::ask;
    context.expect(
        format_order_book_csv_row(ask_only, buffer) ==
                CsvFormatError::none &&
            buffer.view().find(",D,S,") != std::string_view::npos,
        "ask-only side code serializes as uppercase S");
}

void test_order_book_validation(Context& context) {
    constexpr std::array<BookLevel, 6U> six_levels{{
        BookLevel{6, 1},
        BookLevel{5, 1},
        BookLevel{4, 1},
        BookLevel{3, 1},
        BookLevel{2, 1},
        BookLevel{1, 1},
    }};
    constexpr std::array<BookLevel, 1U> valid_ask{{
        BookLevel{7, 1},
    }};
    constexpr std::array<BookLevel, 2U> unordered_bids{{
        BookLevel{5, 1},
        BookLevel{6, 1},
    }};
    constexpr std::array<BookLevel, 1U> zero_quantity{{
        BookLevel{6, 0},
    }};
    constexpr std::array<BookLevel, 1U> crossed_ask{{
        BookLevel{6, 1},
    }};
    CsvRecordBuffer buffer;
    OrderBookCsvRow row{
        CsvTimestamp{1, 0},
        1U,
        1,
        BookRowType::differential,
        BookRowSide::neutral,
        BookSideView{six_levels.data(), 1U},
        BookSideView{valid_ask.data(), valid_ask.size()},
    };

    row.sequence_number = 0U;
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_sequence &&
            buffer.empty(),
        "zero order-book sequence is rejected");
    row.sequence_number = 1U;

    row.instrument_id = 0;
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_instrument_id &&
            buffer.empty(),
        "zero instrument ID is rejected");
    row.instrument_id = 1;

    row.bids = BookSideView{six_levels.data(), six_levels.size()};
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_book &&
            buffer.empty(),
        "more than five visible levels is rejected");
    row.bids = BookSideView{six_levels.data(), 1U};

    row.bids =
        BookSideView{unordered_bids.data(), unordered_bids.size()};
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_book &&
            buffer.empty(),
        "unordered visible levels are rejected");
    row.bids = BookSideView{six_levels.data(), 1U};

    row.bids =
        BookSideView{zero_quantity.data(), zero_quantity.size()};
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_book &&
            buffer.empty(),
        "zero-quantity visible levels are rejected");
    row.bids = BookSideView{six_levels.data(), 1U};

    row.asks =
        BookSideView{crossed_ask.data(), crossed_ask.size()};
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_book &&
            buffer.empty(),
        "crossed visible book is rejected");
    row.asks = BookSideView{valid_ask.data(), valid_ask.size()};

    row.bids = BookSideView{nullptr, 1U};
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_book &&
            buffer.empty(),
        "nonempty null book view is rejected");
    row.bids = BookSideView{six_levels.data(), 1U};

    row.row_type = static_cast<BookRowType>(255U);
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_enum &&
            buffer.empty(),
        "invalid order-book row type is rejected");
    row.row_type = BookRowType::differential;

    row.side = static_cast<BookRowSide>(255U);
    context.expect(
        format_order_book_csv_row(row, buffer) ==
                CsvFormatError::invalid_enum &&
            buffer.empty(),
        "invalid side code is rejected");
}

}  // namespace

void run_csv_formatter_tests(Context& context) {
    test_exact_headers(context);
    test_market_data_row_escaping(context);
    test_market_data_validation_and_limit(context);
    test_order_book_row(context);
    test_order_book_validation(context);
}

}  // namespace hft::test
