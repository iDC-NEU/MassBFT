cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME liberasurecode
        GITHUB_REPOSITORY sydxsty/liberasurecode
        GIT_TAG "origin/master"
        # GIT_TAG 031569e00f61dc151de4aa07c88d1740951b730e just # download the newest
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

if(NOT EXISTS "${PROJECT_BINARY_DIR}/lib/liberasurecode.a")
    message("Start configure libErasureCode")
    include(ProcessorCount)
    ProcessorCount(N)
    execute_process(COMMAND ${liberasurecode_SOURCE_DIR}/autogen.sh
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Configure for libErasureCode failed: ${result}")
    endif()
    # Call CMake to generate makefile
    execute_process(COMMAND ${liberasurecode_SOURCE_DIR}/configure --prefix=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Configure for libErasureCode failed: ${result}")
    endif()
    message("Start building libErasureCode")
    # build and install module
    execute_process(COMMAND make -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for libErasureCode failed: ${result}")
    endif()
    message("Start installing libErasureCode")
    execute_process(COMMAND make install
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for libErasureCode failed: ${result}")
    endif()

    message("Start copy libgrc file")
    execute_process(COMMAND cp -f libgrc.so ${PROJECT_BINARY_DIR}/lib
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for libErasureCode failed: ${result}")
    endif()
    execute_process(COMMAND cp -f libgrc.h ${PROJECT_BINARY_DIR}/include
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${liberasurecode_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for libErasureCode failed: ${result}")
    endif()
endif()