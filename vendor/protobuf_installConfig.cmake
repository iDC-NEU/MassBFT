cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME protobuf
        URL https://github.com/protocolbuffers/protobuf/releases/download/v3.16.3/protobuf-all-3.16.3.tar.gz
        VERSION v3.16.3
        DOWNLOAD_ONLY True
)

set(PROTOBUF_PROTOC_EXECUTABLE "${PROJECT_BINARY_DIR}/bin/protoc")

if(NOT EXISTS "${PROTOBUF_PROTOC_EXECUTABLE}")
    message("Start configure protobuf")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${protobuf_SOURCE_DIR}/autogen.sh
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for protobuf failed: ${result}")
    endif()
    execute_process(COMMAND ${protobuf_SOURCE_DIR}/configure --prefix=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for protobuf failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND make -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for protobuf failed: ${result}")
    endif()

    execute_process(COMMAND make install
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for protobuf failed: ${result}")
    endif()
endif()
