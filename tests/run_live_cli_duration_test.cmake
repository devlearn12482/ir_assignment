if(NOT DEFINED BINANCE_CAPTURE OR NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "BINANCE_CAPTURE and BINARY_DIR are required")
endif()

set(output_directory "${BINARY_DIR}/live-cli-duration-output")
file(REMOVE_RECURSE "${output_directory}")

execute_process(
    COMMAND
        "${BINANCE_CAPTURE}"
        --venue spot
        --symbols BTCUSDT,ETHUSDT
        --duration 1
        --output-dir "${output_directory}"
    RESULT_VARIABLE capture_status
    OUTPUT_VARIABLE capture_stdout
    ERROR_VARIABLE capture_stderr
    # ASan startup, live DNS/TLS, and checked writer drain can dominate the
    # one-second capture interval on slow mounted CI/workspace filesystems.
    TIMEOUT 45
)

if(NOT capture_status MATCHES "^[0-9]+$")
    message(FATAL_ERROR
        "bounded live CLI did not return a numeric status (${capture_status})\n"
        "stdout:\n${capture_stdout}\n"
        "stderr:\n${capture_stderr}")
endif()

string(REGEX MATCHALL "METRICS_BEGIN version=1" metrics_begin
    "${capture_stderr}")
string(REGEX MATCHALL "METRICS_END" metrics_end
    "${capture_stderr}")
list(LENGTH metrics_begin metrics_begin_count)
list(LENGTH metrics_end metrics_end_count)
if(NOT metrics_begin_count EQUAL 1 OR NOT metrics_end_count EQUAL 1)
    message(FATAL_ERROR
        "live CLI did not emit exactly one metrics block:\n"
        "${capture_stderr}")
endif()

string(REGEX MATCH "connections.successful=([0-9]+)"
    successful_connection_line "${capture_stderr}")
if(successful_connection_line STREQUAL "")
    message(FATAL_ERROR
        "live CLI metrics omitted connections.successful:\n"
        "${capture_stderr}")
endif()
set(successful_connections "${CMAKE_MATCH_1}")

if(successful_connections EQUAL 0)
    set(expected_exit_code 5)
    set(expected_status fatal)
    set(expected_control_failure no_successful_connection)
else()
    set(expected_exit_code 0)
    set(expected_status success)
    set(expected_control_failure none)
endif()

if(NOT capture_status EQUAL expected_exit_code)
    message(FATAL_ERROR
        "bounded live CLI returned ${capture_status} with "
        "${successful_connections} successful connections; expected "
        "${expected_exit_code}\nstdout:\n${capture_stdout}\n"
        "stderr:\n${capture_stderr}")
endif()

foreach(expected_line
    "run.mode=live"
    "run.status=${expected_status}"
    "run.exit_code=${expected_exit_code}"
    "run.stop_requested=1"
    "run.signals_received=0"
    "run.duration_expired=1"
    "writer.audit_rows_unwritten=0"
    "writer.book_rows_unwritten=0"
    "failure.capture=none"
    "failure.control=${expected_control_failure}"
    "failure.session=none"
    "connections.last_session_result=")
    string(FIND "${capture_stderr}" "${expected_line}" position)
    if(position EQUAL -1)
        message(FATAL_ERROR
            "live CLI metrics omitted ${expected_line}:\n"
            "${capture_stderr}")
    endif()
endforeach()

foreach(symbol BTCUSDT ETHUSDT)
    file(GLOB audit_files
        "${output_directory}/market_data_spot_${symbol}_????-??-??.csv")
    file(GLOB book_files
        "${output_directory}/market_data_spot_${symbol}_????-??-??_orderbook.csv")
    list(LENGTH audit_files audit_count)
    list(LENGTH book_files book_count)
    if(NOT audit_count EQUAL 1 OR NOT book_count EQUAL 1)
        message(FATAL_ERROR
            "live CLI did not create one audit/book pair for ${symbol}")
    endif()
endforeach()
