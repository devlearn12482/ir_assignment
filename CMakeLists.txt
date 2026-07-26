cmake_minimum_required(VERSION 3.22)

project(
    binance_market_data
    VERSION 0.1.0
    DESCRIPTION "Deterministic Binance market-data capture and partial order book"
    LANGUAGES CXX
)

find_package(Boost 1.74 REQUIRED COMPONENTS system)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(HFT_WARNINGS_AS_ERRORS "Treat warnings in project-owned targets as errors" OFF)

function(hft_enable_project_warnings target)
    if(MSVC)
        target_compile_options(
            "${target}"
            PRIVATE
                /W4
                /permissive-
                $<$<BOOL:${HFT_WARNINGS_AS_ERRORS}>:/WX>
        )
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(
            "${target}"
            PRIVATE
                -Wall
                -Wextra
                -Wpedantic
                -Wconversion
                -Wsign-conversion
                -Wshadow
                $<$<BOOL:${HFT_WARNINGS_AS_ERRORS}>:-Werror>
        )
    endif()
endfunction()

add_library(hft_core STATIC
    src/buffered_file_sink.cpp
    src/command_line.cpp
    src/csv_formatter.cpp
    src/csv_output_set.cpp
    src/event_processor.cpp
    src/fixed_point.cpp
    src/live_envelope.cpp
    src/live_subscription.cpp
    src/market_data_replay.cpp
    src/spot_book.cpp
    src/spot_payload_parser.cpp
    src/symbol_identity.cpp
    src/verified_websocket_session.cpp
)

target_compile_features(hft_core PUBLIC cxx_std_17)
target_include_directories(
    hft_core
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
)
hft_enable_project_warnings(hft_core)

add_library(simdjson_vendor STATIC
    third_party/simdjson/simdjson.cpp
)
add_library(simdjson::simdjson ALIAS simdjson_vendor)

target_compile_features(simdjson_vendor PUBLIC cxx_std_17)
target_include_directories(
    simdjson_vendor
    SYSTEM PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/third_party/simdjson"
)

if(MSVC)
    target_compile_options(simdjson_vendor PRIVATE /w)
elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(simdjson_vendor PRIVATE -w)
endif()

target_link_libraries(
    hft_core
    PUBLIC
        Boost::system
        OpenSSL::Crypto
        OpenSSL::SSL
        Threads::Threads
    PRIVATE
        simdjson::simdjson
)

add_executable(binance_capture src/main.cpp)
target_link_libraries(binance_capture PRIVATE hft_core simdjson::simdjson)
hft_enable_project_warnings(binance_capture)

include(CTest)

if(BUILD_TESTING)
    add_subdirectory(tests)

    set(hft_ctest_configuration)
    if(CMAKE_CONFIGURATION_TYPES)
        set(hft_ctest_configuration --build-config "$<CONFIG>")
    endif()

    add_custom_target(
        tests
        COMMAND
            "${CMAKE_CTEST_COMMAND}"
            ${hft_ctest_configuration}
            --output-on-failure
        DEPENDS
            unit_tests
            binance_capture
            tls_session_integration_test
            websocket_read_loop_integration_test
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        USES_TERMINAL
        COMMENT "Running baseline unit tests"
    )
endif()
