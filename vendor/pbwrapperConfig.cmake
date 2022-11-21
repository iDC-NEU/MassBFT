cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME protobuf
        GITHUB_REPOSITORY protocolbuffers/protobuf
        GIT_TAG 847ace296ca55574e22c7c458b1cbee6bf7e4789
        VERSION v3.21.4.0
        OPTIONS "protobuf_BUILD_TESTS OFF"
)

set(PROTOBUF_PROTOC_EXECUTABLE "${PROJECT_BINARY_DIR}/bin/protoc")

if(NOT EXISTS "${PROTOBUF_PROTOC_EXECUTABLE}")
    message("Start configure protobuf")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for protobuf failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for protobuf failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${protobuf_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for protobuf failed: ${result}")
    endif()
endif()
