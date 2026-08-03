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

if(NOT capture_status EQUAL 0)
    message(FATAL_ERROR
        "bounded live CLI failed (${capture_status})\n"
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

foreach(expected_line
    "run.mode=live"
    "run.status=success"
    "run.exit_code=0"
    "run.stop_requested=1"
    "run.signals_received=0"
    "run.duration_expired=1"
    "writer.audit_rows_unwritten=0"
    "writer.book_rows_unwritten=0"
    "failure.capture=none"
    "failure.control=none")
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
