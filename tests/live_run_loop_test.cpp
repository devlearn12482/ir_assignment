#include "test_framework.h"

#include "hft/live_run_loop.h"

#include <boost/asio/io_context.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>

namespace hft::test {
namespace {

void test_duration_validation(Context& context) {
    boost::asio::io_context io_context;
    LiveRunLoopCreateError error;
    std::shared_ptr<LiveRunLoop> loop =
        LiveRunLoop::create(io_context, std::nullopt, error);
    context.expect(
        loop != nullptr && error == LiveRunLoopCreateError::none,
        "run loop accepts an unbounded signal-driven run");
    loop.reset();

    loop = LiveRunLoop::create(
        io_context, std::optional<std::uint64_t>{0U}, error);
    context.expect(
        loop == nullptr &&
            error == LiveRunLoopCreateError::invalid_duration,
        "run loop rejects a zero duration");

    loop = LiveRunLoop::create(
        io_context,
        std::optional<std::uint64_t>{
            std::numeric_limits<std::uint64_t>::max()},
        error);
    context.expect(
        loop == nullptr &&
            error == LiveRunLoopCreateError::invalid_duration,
        "run loop rejects duration conversion overflow");
}

}  // namespace

void run_live_run_loop_tests(Context& context) {
    test_duration_validation(context);
}

}  // namespace hft::test
