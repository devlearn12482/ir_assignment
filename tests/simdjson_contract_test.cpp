#include "test_framework.h"

#include <cstddef>
#include <string_view>
#include <vector>

#include <simdjson.h>

namespace hft::test {

void run_simdjson_contract_tests(Context& context) {
    static_assert(simdjson::SIMDJSON_VERSION_MAJOR == 3);
    static_assert(simdjson::SIMDJSON_VERSION_MINOR == 6);
    static_assert(simdjson::SIMDJSON_VERSION_REVISION == 4);

    context.expect(
        std::string_view{SIMDJSON_VERSION} == "3.6.4",
        "the vendored simdjson version is exactly 3.6.4");

    constexpr std::string_view source = R"json(
        {
          "stream": "btcusdt@trade",
          "data": {
            "z": 1.2300e+02,
            "text": "A\\n\\u0042\\\"",
            "array": [3, 2, 1],
            "a": -0.00000000
          }
        }
    )json";

    constexpr std::string_view expected =
        R"json({"z":1.2300e+02,"text":"A\\n\\u0042\\\"","array":[3,2,1],"a":-0.00000000})json";

    simdjson::ondemand::parser parser;
    simdjson::padded_string padded_source{source};
    simdjson::ondemand::document document;

    const simdjson::error_code iterate_error =
        parser.iterate(padded_source).get(document);
    context.expect(
        iterate_error == simdjson::SUCCESS,
        "ondemand::parser::iterate succeeds");
    if (iterate_error != simdjson::SUCCESS) {
        return;
    }

    std::string_view raw_data;
    const simdjson::error_code raw_error =
        document["data"].raw_json().get(raw_data);
    context.expect(
        raw_error == simdjson::SUCCESS,
        "ondemand::value::raw_json succeeds");
    if (raw_error != simdjson::SUCCESS) {
        return;
    }

    std::vector<char> output(raw_data.size());
    std::size_t output_length{0};
    const simdjson::error_code minify_error = simdjson::minify(
        raw_data.data(),
        raw_data.size(),
        output.data(),
        output_length);
    context.expect(
        minify_error == simdjson::SUCCESS,
        "output-buffer simdjson::minify succeeds");
    if (minify_error != simdjson::SUCCESS) {
        return;
    }

    const std::string_view actual{output.data(), output_length};
    context.expect(
        actual == expected,
        "minification removes only insignificant whitespace and preserves "
        "numeric lexemes, string escapes, array order, and object-key order");
}

}  // namespace hft::test
