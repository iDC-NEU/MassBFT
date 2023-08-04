cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME yaml-cpp
        GITHUB_REPOSITORY jbeder/yaml-cpp
        VERSION 0.7.0
        GIT_TAG yaml-cpp-0.7.0
        DOWNLOAD_ONLY True
        GIT_SHALLOW TRUE
)

set(YAML-CPP_LIB "${PROJECT_BINARY_DIR}/include/yaml-cpp/yaml.h")

if(NOT EXISTS "${YAML-CPP_LIB}")
    message("Start configure yaml-cpp")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DYAML_CPP_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${yaml-cpp_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for yaml-cpp failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${yaml-cpp_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for yaml-cpp failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${yaml-cpp_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for yaml-cpp failed: ${result}")
    endif()
endif()
