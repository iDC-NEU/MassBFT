cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME libssh
        GIT_REPOSITORY https://git.libssh.org/projects/libssh.git
        GIT_TAG libssh-0.10.4
        DOWNLOAD_ONLY True
)

set(LIBSSH_LIB "${PROJECT_BINARY_DIR}/lib/libssh.a")

if(NOT EXISTS "${LIBSSH_LIB}")
    message("Start configure libssh")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${libssh_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for libssh failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${libssh_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for libssh failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${libssh_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for libssh failed: ${result}")
    endif()
endif()
