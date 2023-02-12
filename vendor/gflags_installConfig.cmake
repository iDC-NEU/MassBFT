cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME gflags
        GITHUB_REPOSITORY gflags/gflags
        VERSION 2.2.3
        GIT_TAG a738fdf9338412f83ab3f26f31ac11ed3f3ec4bd
        DOWNLOAD_ONLY True
)

set(GFLAGS_LIB "${PROJECT_BINARY_DIR}/include/gflags/gflags_declare.h")

if(NOT EXISTS "${GFLAGS_LIB}")
    message("Start configure gflags")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_SHARED_LIBS=ON -DBUILD_STATIC_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${gflags_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for gflags failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${gflags_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for gflags failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${gflags_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for gflags failed: ${result}")
    endif()
endif()
