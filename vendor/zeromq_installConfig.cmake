cmake_minimum_required(VERSION 3.14...3.22)

CPMAddPackage(
        NAME zeromq
        URL https://github.com/zeromq/libzmq/archive/ec013f3a17beaa475c18e8cf5e93970800e2f94a.tar.gz
        VERSION v22.11.20
        DOWNLOAD_ONLY True
)

set(ZMQ_LIB "${PROJECT_BINARY_DIR}/lib/libzmq.a")

if(NOT EXISTS "${ZMQ_LIB}")
    message("Start configure zeromq")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release -DZMQ_BUILD_TESTS=OFF -DENABLE_CURVE=OFF -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
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
        URL https://github.com/zeromq/cppzmq/archive/945d60c36b6cbd4f47b13d999137963fcc78a743.tar.gz
        VERSION v4.9.0
        DOWNLOAD_ONLY True
)

set(CPPZMQ_LIB "${PROJECT_BINARY_DIR}/include/zmq.hpp")

if(NOT EXISTS "${CPPZMQ_LIB}")
    message("Start configure cppzmq")
    include(ProcessorCount)
    ProcessorCount(N)
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build -DCMAKE_BUILD_TYPE=Release-DCPPZMQ_BUILD_TESTS=OFF -DCMAKE_PREFIX_PATH=${PROJECT_BINARY_DIR} -DCMAKE_INSTALL_PREFIX=${PROJECT_BINARY_DIR}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cppzmq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "CMake step for cppzmq failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cppzmq_SOURCE_DIR})
    if(result)
        message(FATAL_ERROR "Install step for cppzmq failed: ${result}")
    endif()
endif()