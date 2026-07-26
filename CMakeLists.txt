cmake_minimum_required(VERSION 3.22)

project(
    binance_market_data
    VERSION 0.1.0
    DESCRIPTION "Deterministic Binance market-data capture and partial order book"
    LANGUAGES CXX
)

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
        DEPENDS unit_tests
        WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
        USES_TERMINAL
        COMMENT "Running baseline unit tests"
    )
endif()
