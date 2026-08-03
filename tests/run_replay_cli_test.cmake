if(NOT DEFINED BINANCE_CAPTURE OR
   NOT DEFINED SOURCE_DIR OR
   NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "replay CLI test arguments are incomplete")
endif()

set(output_dir "${BINARY_DIR}/replay_cli_output")
file(REMOVE_RECURSE "${output_dir}")

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --replay
        "${SOURCE_DIR}/testdata/replay/market_data_spot_BTCUSDT_fixture.csv"
        --replay
        "${SOURCE_DIR}/testdata/replay/market_data_usdm_ETHUSDT_fixture.csv"
        --output-dir
        "${output_dir}"
    RESULT_VARIABLE replay_result
    OUTPUT_VARIABLE replay_output
    ERROR_VARIABLE replay_error
)
if(NOT replay_result EQUAL 0)
    message(
        FATAL_ERROR
        "public replay command failed (${replay_result})\n"
        "stdout: ${replay_output}\n"
        "stderr: ${replay_error}"
    )
endif()
if(NOT replay_output MATCHES
   "replay_complete files=2 rows_read=[0-9]+ rows_processed=[0-9]+ order_book_rows=[0-9]+")
    message(FATAL_ERROR "success summary is missing or unstable: ${replay_output}")
endif()
string(REGEX MATCHALL "METRICS_BEGIN version=1" replay_metrics_begin
    "${replay_error}")
string(REGEX MATCHALL "METRICS_END" replay_metrics_end
    "${replay_error}")
list(LENGTH replay_metrics_begin replay_metrics_begin_count)
list(LENGTH replay_metrics_end replay_metrics_end_count)
if(NOT replay_metrics_begin_count EQUAL 1 OR
   NOT replay_metrics_end_count EQUAL 1 OR
   NOT replay_error MATCHES "run.mode=replay" OR
   NOT replay_error MATCHES "run.status=success" OR
   NOT replay_error MATCHES "connections.attempts=0" OR
   NOT replay_error MATCHES "writer.audit_rows_written=0" OR
   NOT replay_error MATCHES "writer.book_rows_unwritten=0")
    message(FATAL_ERROR
        "replay metrics block is missing or inconsistent: ${replay_error}")
endif()

foreach(stem
    market_data_spot_BTCUSDT_fixture
    market_data_usdm_ETHUSDT_fixture)
    execute_process(
        COMMAND
            "${CMAKE_COMMAND}" -E compare_files
            "${output_dir}/${stem}_orderbook.csv"
            "${SOURCE_DIR}/testdata/replay/expected/${stem}_orderbook.csv"
        RESULT_VARIABLE compare_result
    )
    if(NOT compare_result EQUAL 0)
        message(FATAL_ERROR "${stem} replay output differs byte-for-byte")
    endif()
endforeach()

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --replay
        "${SOURCE_DIR}/testdata/replay/market_data_spot_BTCUSDT_fixture.csv"
        --output-dir
        "${output_dir}"
    RESULT_VARIABLE collision_result
    OUTPUT_QUIET
    ERROR_VARIABLE collision_error
)
if(collision_result EQUAL 0)
    message(FATAL_ERROR "nonempty output directory was unexpectedly accepted")
endif()
if(NOT collision_error MATCHES "output_directory_not_empty")
    message(
        FATAL_ERROR
        "collision failure lacked stable category: ${collision_error}"
    )
endif()

file(REMOVE_RECURSE "${output_dir}")

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --replay
        "${SOURCE_DIR}/testdata/replay/market_data_spot_BTCUSDT_fixture.csv"
        --venue
        spot
        --output-dir
        "${output_dir}"
    RESULT_VARIABLE mixed_result
    OUTPUT_QUIET
    ERROR_VARIABLE mixed_error
)
if(NOT mixed_result EQUAL 2 OR NOT mixed_error MATCHES "mixed_modes")
    message(
        FATAL_ERROR
        "mixed-mode rejection was not stable: rc=${mixed_result} ${mixed_error}"
    )
endif()

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --replay
        "${SOURCE_DIR}/testdata/replay/expected/market_data_spot_BTCUSDT_fixture_orderbook.csv"
        --output-dir
        "${output_dir}"
    RESULT_VARIABLE malformed_result
    OUTPUT_QUIET
    ERROR_VARIABLE malformed_error
)
if(NOT malformed_result EQUAL 3 OR
   NOT malformed_error MATCHES "invalid_header")
    message(
        FATAL_ERROR
        "invalid replay input was not classified before output creation: "
        "rc=${malformed_result} ${malformed_error}"
    )
endif()
if(EXISTS "${output_dir}")
    message(FATAL_ERROR "invalid replay input created the output directory")
endif()

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --replay
        "${SOURCE_DIR}/testdata/replay/market_data_spot_BTCUSDT_fixture.csv"
        --output-dir
        "${SOURCE_DIR}/testdata/replay"
    RESULT_VARIABLE same_directory_result
    OUTPUT_QUIET
    ERROR_VARIABLE same_directory_error
)
if(NOT same_directory_result EQUAL 4 OR
   NOT same_directory_error MATCHES "output_matches_input_directory")
    message(
        FATAL_ERROR
        "same-directory rejection was not stable: "
        "rc=${same_directory_result} ${same_directory_error}"
    )
endif()
