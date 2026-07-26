#include <simdjson.h>

#include <cstddef>
#include <iostream>
#include <string_view>
#include <vector>

namespace {

class TestContext {
public:
    void expect(const bool condition, const std::string_view message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    void expect_success(
        const simdjson::error_code error,
        const std::string_view operation) {
        if (error != simdjson::SUCCESS) {
            ++failures_;
            std::cerr << "FAIL: " << operation << ": "
                      << simdjson::error_message(error) << '\n';
        }
    }

    [[nodiscard]] int result() const noexcept {
        return failures_ == 0 ? 0 : 1;
    }

private:
    int failures_{0};
};

void test_pinned_simdjson_contract(TestContext& context) {
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
    context.expect_success(iterate_error, "ondemand::parser::iterate");
    if (iterate_error != simdjson::SUCCESS) {
        return;
    }

    std::string_view raw_data;
    const simdjson::error_code raw_error =
        document["data"].raw_json().get(raw_data);
    context.expect_success(raw_error, "ondemand::value::raw_json");
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
    context.expect_success(minify_error, "output-buffer simdjson::minify");
    if (minify_error != simdjson::SUCCESS) {
        return;
    }

    const std::string_view actual{output.data(), output_length};
    context.expect(
        actual == expected,
        "minification removes only insignificant whitespace and preserves "
        "numeric lexemes, string escapes, array order, and object-key order");
}
}  // namespace

int main() {
    TestContext context;
    test_pinned_simdjson_contract(context);

    if (context.result() == 0) {
        std::cout << "PASS: simdjson 3.6.4 dependency contract\n";
    }
    return context.result();
}
