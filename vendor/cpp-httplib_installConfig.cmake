cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME cpp-httplib
        GITHUB_REPOSITORY yhirose/cpp-httplib
        VERSION 0.13.3
        GIT_TAG v0.13.3
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

set(CPP-HTTPLIB_LIB "${CMAKE_BINARY_DIR}/include/httplib.h")

if(NOT EXISTS "${CPP-HTTPLIB_LIB}")
    message("Start configure cpp-httplib")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cpp-httplib_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for cpp-httplib failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cpp-httplib_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for cpp-httplib failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cpp-httplib_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for cpp-httplib failed: ${result}")
    endif()
endif()
