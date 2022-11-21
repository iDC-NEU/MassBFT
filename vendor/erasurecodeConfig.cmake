cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME liberasurecode
        GITHUB_REPOSITORY openstack/liberasurecode
        VERSION v1.6.3
        GIT_TAG 296cf48e5a78b0e2a5f1b768585112b7f7f24e5d
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
endif()
