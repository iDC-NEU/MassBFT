cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME nljson
        URL https://github.com/nlohmann/json/archive/refs/tags/v3.11.2.tar.gz
        VERSION 3.11.2
        DOWNLOAD_ONLY True
)

set(NLJSON_LIB "${PROJECT_BINARY_DIR}/include/nlohmann/json.hpp")

if(NOT EXISTS "${NLJSON_LIB}")
    message("Start configure nljson")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DJSON_BuildTests=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nljson_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for nljson failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nljson_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for nljson failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${nljson_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for nljson failed: ${result}")
    endif()
endif()
