cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME zeromq
        GITHUB_REPOSITORY zeromq/libzmq
        VERSION 22.11.20
        GIT_TAG 8d4f4efef0b45a242c0b0f927d715ac79d863729
        DOWNLOAD_ONLY True
)

set(ZMQ_LIB "${PROJECT_BINARY_DIR}/lib/libzmq.a")

if(NOT EXISTS "${ZMQ_LIB}")
    message("Start configure zeromq")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DZMQ_BUILD_TESTS=OFF -DENABLE_CURVE=OFF -DWITH_TLS=OFF -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${zeromq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for zeromq failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${zeromq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for zeromq failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${zeromq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for zeromq failed: ${result}")
    endif()
endif()

CPMAddPackage(
        NAME cppzmq
        GITHUB_REPOSITORY zeromq/cppzmq
        VERSION v4.9.0
        GIT_TAG c66fc6094b2a03439dea8469719e221e80e2e8e7
        DOWNLOAD_ONLY True
)

set(CPPZMQ_LIB "${PROJECT_BINARY_DIR}/include/zmq.hpp")

if(NOT EXISTS "${CPPZMQ_LIB}")
    message("Start configure cppzmq")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cppzmq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for cppzmq failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cppzmq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Build step for cppzmq failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cppzmq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for cppzmq failed: ${result}")
    endif()
endif()