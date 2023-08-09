cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME openssl
        URL https://github.com/openssl/openssl/releases/download/openssl-3.1.2/openssl-3.1.2.tar.gz
        VERSION v3.1.2
        DOWNLOAD_ONLY True
)

if(NOT EXISTS "${PROJECT_BINARY_DIR}/bin/openssl")
    message("Start configure openssl")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${openssl_SOURCE_DIR}/Configure --prefix=${PROJECT_BINARY_DIR} --libdir=${PROJECT_BINARY_DIR}/lib --openssldir=${PROJECT_BINARY_DIR}/crypto
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
            OUTPUT_QUIET)
    if(result)
        message(FATAL_ERROR "Configure for openssl failed: ${result}")
    endif()
    message("Start building openssl")
    # build and install module
    execute_process(COMMAND make build_sw -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
            OUTPUT_QUIET)
    if(result)
        message(FATAL_ERROR "Build step for openssl failed: ${result}")
    endif()
    message("Start installing openssl")
    execute_process(COMMAND make install_sw
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${openssl_SOURCE_DIR}
            OUTPUT_QUIET)
    if(result)
        message(FATAL_ERROR "Install step for openssl failed: ${result}")
    endif()
endif()

set(OPENSSL_ROOT_DIR ${PROJECT_BINARY_DIR})
