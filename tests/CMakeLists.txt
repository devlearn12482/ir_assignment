add_executable(unit_tests
    csv_formatter_test.cpp
    event_processor_test.cpp
    fixed_point_test.cpp
    simdjson_contract_test.cpp
    spot_book_test.cpp
    spot_payload_parser_test.cpp
    symbol_identity_test.cpp
    unit_tests.cpp
    usdm_book_test.cpp
    usdm_payload_parser_test.cpp
)

target_include_directories(unit_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
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
