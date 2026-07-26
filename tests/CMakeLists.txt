add_executable(unit_tests
    buffered_file_sink_test.cpp
    command_line_test.cpp
    csv_formatter_test.cpp
    csv_output_set_test.cpp
    event_processor_test.cpp
    event_processor_robustness_test.cpp
    fixed_point_test.cpp
    live_envelope_test.cpp
    live_subscription_test.cpp
    market_data_replay_test.cpp
    simdjson_contract_test.cpp
    spot_book_test.cpp
    spot_payload_parser_test.cpp
    symbol_identity_test.cpp
    unit_tests.cpp
    usdm_book_test.cpp
    usdm_payload_parser_test.cpp
    verified_websocket_session_test.cpp
)

add_executable(
    tls_session_integration_test
    tls_session_integration_test.cpp
)
target_link_libraries(tls_session_integration_test PRIVATE hft_core)
hft_enable_project_warnings(tls_session_integration_test)

target_include_directories(unit_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_definitions(
    unit_tests
    PRIVATE HFT_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
)
target_link_libraries(unit_tests PRIVATE hft_core simdjson::simdjson)
hft_enable_project_warnings(unit_tests)

set_target_properties(
    unit_tests
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
)

foreach(configuration Debug Release RelWithDebInfo MinSizeRel)
    string(TOUPPER "${configuration}" configuration_upper)
    set_target_properties(
        unit_tests
        PROPERTIES
            "RUNTIME_OUTPUT_DIRECTORY_${configuration_upper}" "${CMAKE_BINARY_DIR}/tests"
    )
endforeach()

add_test(NAME unit_tests COMMAND unit_tests)

find_program(HFT_OPENSSL_EXECUTABLE openssl REQUIRED)
add_test(
    NAME verified_tls_websocket
    COMMAND
        "${CMAKE_COMMAND}"
        -DTEST_EXECUTABLE=$<TARGET_FILE:tls_session_integration_test>
        -DOPENSSL_EXECUTABLE=${HFT_OPENSSL_EXECUTABLE}
        -DBINARY_DIR=${CMAKE_BINARY_DIR}
        -P
        "${CMAKE_CURRENT_SOURCE_DIR}/run_tls_session_integration.cmake"
)

add_test(
    NAME replay_cli
    COMMAND
        "${CMAKE_COMMAND}"
        -DBINANCE_CAPTURE=$<TARGET_FILE:binance_capture>
        -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
        -DBINARY_DIR=${CMAKE_BINARY_DIR}
        -P
        "${CMAKE_CURRENT_SOURCE_DIR}/run_replay_cli_test.cmake"
)

find_program(HFT_SHA256SUM_EXECUTABLE sha256sum)
if(HFT_SHA256SUM_EXECUTABLE)
    add_test(
        NAME replay_fixture_hashes
        COMMAND
            "${HFT_SHA256SUM_EXECUTABLE}"
            --check
            SHA256SUMS
        WORKING_DIRECTORY
            "${CMAKE_SOURCE_DIR}/testdata/replay"
    )
endif()
