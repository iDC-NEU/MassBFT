cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME braft
        GITHUB_REPOSITORY sydxsty/braft
        # GIT_TAG "origin/master"
        VERSION 1.1.3
        GIT_TAG b684d246e205c6fa1d0701c483e09a1abdb0ed25
        DOWNLOAD_ONLY True
)

set(BRAFT_LIB "${PROJECT_BINARY_DIR}/lib/libbraft.a")

if(NOT EXISTS "${BRAFT_LIB}")
    message("Start configure braft")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DBRPC_WITH_GLOG=ON -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${braft_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for braft failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --target braft-static braft-shared --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${braft_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for braft failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${braft_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for braft failed: ${result}")
    endif()
endif()
