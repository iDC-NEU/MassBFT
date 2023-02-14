cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME googletest
        GITHUB_REPOSITORY google/googletest
        GIT_TAG v1.13.0
        VERSION 1.13.0
        OPTIONS "INSTALL_GTEST OFF" "gtest_force_shared_crt"
        GIT_SHALLOW TRUE
)

# We only need the header, the other parts are not useful
set(GTEST_LIB "${PROJECT_BINARY_DIR}/lib/libgmock_main.a")

if(NOT EXISTS "${GTEST_LIB}")
    message("Start configure googletest")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -Dgtest_force_shared_crt=ON -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for googletest failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for googletest failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${googletest_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for googletest failed: ${result}")
    endif()
endif()
