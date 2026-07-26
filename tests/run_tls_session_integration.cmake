if(NOT DEFINED TEST_EXECUTABLE OR
   NOT DEFINED OPENSSL_EXECUTABLE OR
   NOT DEFINED BINARY_DIR)
    message(FATAL_ERROR "TLS integration arguments are incomplete")
endif()

set(tls_dir "${BINARY_DIR}/ephemeral_tls")
file(REMOVE_RECURSE "${tls_dir}")
file(MAKE_DIRECTORY "${tls_dir}")
set(ca_key "${tls_dir}/ca.key")
set(ca_cert "${tls_dir}/ca.pem")
set(server_key "${tls_dir}/server.key")
set(server_csr "${tls_dir}/server.csr")
set(server_cert "${tls_dir}/server.pem")
set(extensions "${tls_dir}/server.ext")
file(
    WRITE
    "${extensions}"
    "subjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth\n"
)

execute_process(
    COMMAND
        "${OPENSSL_EXECUTABLE}" req -x509 -newkey rsa:2048 -nodes
        -keyout "${ca_key}"
        -out "${ca_cert}"
        -subj "/CN=HFT Ephemeral Test CA"
        -days 1
        -sha256
    RESULT_VARIABLE ca_result
    OUTPUT_QUIET
    ERROR_VARIABLE ca_error
)
if(NOT ca_result EQUAL 0)
    file(REMOVE_RECURSE "${tls_dir}")
    message(FATAL_ERROR "ephemeral CA generation failed: ${ca_error}")
endif()

execute_process(
    COMMAND
        "${OPENSSL_EXECUTABLE}" req -newkey rsa:2048 -nodes
        -keyout "${server_key}"
        -out "${server_csr}"
        -subj "/CN=localhost"
        -sha256
    RESULT_VARIABLE csr_result
    OUTPUT_QUIET
    ERROR_VARIABLE csr_error
)
if(NOT csr_result EQUAL 0)
    file(REMOVE_RECURSE "${tls_dir}")
    message(FATAL_ERROR "ephemeral CSR generation failed: ${csr_error}")
endif()

execute_process(
    COMMAND
        "${OPENSSL_EXECUTABLE}" x509 -req
        -in "${server_csr}"
        -CA "${ca_cert}"
        -CAkey "${ca_key}"
        -CAcreateserial
        -out "${server_cert}"
        -days 1
        -sha256
        -extfile "${extensions}"
    RESULT_VARIABLE sign_result
    OUTPUT_QUIET
    ERROR_VARIABLE sign_error
)
if(NOT sign_result EQUAL 0)
    file(REMOVE_RECURSE "${tls_dir}")
    message(FATAL_ERROR "ephemeral certificate signing failed: ${sign_error}")
endif()

execute_process(
    COMMAND
        "${TEST_EXECUTABLE}"
        "${server_cert}"
        "${server_key}"
        "${ca_cert}"
    RESULT_VARIABLE test_result
    OUTPUT_VARIABLE test_output
    ERROR_VARIABLE test_error
    TIMEOUT 30
)

file(REMOVE_RECURSE "${tls_dir}")

if(NOT test_result EQUAL 0)
    message(
        FATAL_ERROR
        "verified TLS integration failed (${test_result})\n"
        "stdout: ${test_output}\n"
        "stderr: ${test_error}"
    )
endif()
