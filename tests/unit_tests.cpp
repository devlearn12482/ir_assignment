#include "test_framework.h"

#include <iostream>

int main() {
    hft::test::Context context;
    hft::test::run_buffered_file_sink_tests(context);
    hft::test::run_csv_formatter_tests(context);
    hft::test::run_csv_output_set_tests(context);
    hft::test::run_symbol_identity_tests(context);
    hft::test::run_simdjson_contract_tests(context);
    hft::test::run_fixed_point_tests(context);
    hft::test::run_spot_payload_parser_tests(context);
    hft::test::run_spot_book_tests(context);
    hft::test::run_usdm_payload_parser_tests(context);
    hft::test::run_usdm_book_tests(context);
    hft::test::run_event_processor_tests(context);
    hft::test::run_event_processor_robustness_tests(context);

    if (context.result() == 0) {
        std::cout << "PASS: all unit tests\n";
    }
    return context.result();
}
