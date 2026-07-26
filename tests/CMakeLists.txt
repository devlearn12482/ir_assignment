add_executable(unit_tests
    simdjson_contract_test.cpp
)

target_link_libraries(unit_tests PRIVATE simdjson::simdjson)
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
