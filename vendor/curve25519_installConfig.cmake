cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME curve25519
        GITHUB_REPOSITORY msotoodeh/curve25519
        GIT_TAG 23a656c5234758f50d0576b49e0e9eecff68063b
        DOWNLOAD_ONLY True
)

set(CURVE25519_LIB "${PROJECT_BINARY_DIR}/lib/libcurve25519x64.a")

if(NOT EXISTS "${CURVE25519_LIB}")
    message("Start configure CURVE25519")
    # Call CMake to generate makefile
    execute_process(COMMAND make asm
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${curve25519_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Make step for curve25519 failed: ${result}")
    endif()
    execute_process(COMMAND cp -f ./include/ed25519_signature.h ${PROJECT_BINARY_DIR}/include/
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${curve25519_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Make step for curve25519 failed: ${result}")
    endif()
    execute_process(COMMAND cp -f ./source/asm64/build64/libcurve25519x64.a ${PROJECT_BINARY_DIR}/lib/
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${curve25519_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Make step for curve25519 failed: ${result}")
    endif()
endif()
