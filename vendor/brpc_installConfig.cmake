cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME brpc
        URL https://github.com/apache/brpc/archive/refs/tags/1.6.0.tar.gz
        VERSION v1.6.0
        DOWNLOAD_ONLY True
)

set(BRPC_LIB "${PROJECT_BINARY_DIR}/lib/libbrpc.a")

if(NOT EXISTS "${BRPC_LIB}")
    message("Start configure brpc")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DWITH_GLOG=ON -DDOWNLOAD_GTEST=OFF -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${brpc_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for brpc failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${brpc_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for brpc failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${brpc_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for brpc failed: ${result}")
    endif()
endif()
