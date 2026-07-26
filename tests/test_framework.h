#pragma once

#include <iostream>
#include <string_view>

namespace hft::test {

class Context {
public:
    void expect(const bool condition, const std::string_view message) {
        if (!condition) {
            ++failures_;
            std::cerr << "FAIL: " << message << '\n';
        }
    }

    [[nodiscard]] int result() const noexcept {
        return failures_ == 0 ? 0 : 1;
    }

private:
    int failures_{0};
};

void run_fixed_point_tests(Context& context);
void run_simdjson_contract_tests(Context& context);
void run_spot_book_tests(Context& context);
void run_spot_payload_parser_tests(Context& context);
void run_usdm_book_tests(Context& context);
void run_usdm_payload_parser_tests(Context& context);

}  // namespace hft::test
