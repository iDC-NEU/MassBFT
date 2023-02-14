cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME leveldb
        GITHUB_REPOSITORY google/leveldb
        VERSION 1.2.4
        GIT_TAG fb644cb44539925a7f444b1b0314f402a456c5f4
        DOWNLOAD_ONLY True
)

set(LEVELDB_LIB "${PROJECT_BINARY_DIR}/include/leveldb/db.h")

if(NOT EXISTS "${LEVELDB_LIB}")
    message("Start configure leveldb")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${leveldb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for leveldb failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${leveldb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for leveldb failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${leveldb_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for leveldb failed: ${result}")
    endif()
endif()
