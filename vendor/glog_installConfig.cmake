cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME glog
        GITHUB_REPOSITORY google/glog
        VERSION 0.6.0
        GIT_TAG v0.6.0
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

set(GLOG_LIB "${PROJECT_BINARY_DIR}/include/glog/logging.h")

if(NOT EXISTS "${GLOG_LIB}")
    message("Start configure glog")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_SHARED_LIBS=ON -DWITH_GTEST=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${glog_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for glog failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${glog_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for glog failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${glog_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for glog failed: ${result}")
    endif()
endif()
