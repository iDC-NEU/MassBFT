cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME rocksdb
        GITHUB_REPOSITORY facebook/rocksdb
        VERSION 7.9.2
        GIT_TAG v7.9.2
        GIT_SHALLOW TRUE
        DOWNLOAD_ONLY True
)

set(ROCKSDB_LIB "${PROJECT_BINARY_DIR}/include/rocksdb/db.h")

if(NOT EXISTS "${ROCKSDB_LIB}")
    message("Start configure rocksdb")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DWITH_GFLAGS=ON -DWITH_TESTS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rocksdb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for rocksdb failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rocksdb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for rocksdb failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${rocksdb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for rocksdb failed: ${result}")
    endif()
endif()
