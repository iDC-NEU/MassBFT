cmake_minimum_required(VERSION 3.12)

SET(BUILD_PARAM -DZMQ_BUILD_TESTS=OFF -DENABLE_CPACK=OFF -DWITH_PERF_TOOL=ON -DCMAKE_BUILD_TYPE=Release)

SET(REPO_NAME zeromq)

DOWNLOAD_REPO()

execute_process(COMMAND ./autogen.sh
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME})
if(result)
    message(FATAL_ERROR "Can not download repo: ${result}")
endif()

INSTALL_REPO()