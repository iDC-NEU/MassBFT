cmake_minimum_required(VERSION 3.12)

function(DOWNLOAD_REPO)
    # Download and unpack a cmake repo at configure time
    configure_file(CMakeLists.txt.in src/${REPO_NAME}-download/CMakeLists.txt)

    # 1. Generate a valid download script.
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME}-download)
    if(result)
        message(FATAL_ERROR "Download Makefile generation step failed: ${result}")
    endif()

    # 2. Call CMake to download the repo
    execute_process(COMMAND ${CMAKE_COMMAND} --build .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME}-download)
    if(result)
        message(FATAL_ERROR "Can not download repo: ${result}")
    endif()
endfunction()

function(INSTALL_REPO)
    include(ProcessorCount)
    ProcessorCount(N)
    # now that the repo is copied into ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME}
    # Call CMake to generate makefile
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -B build ${BUILD_PARAM} -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME})
    if(result)
        message(FATAL_ERROR "CMake step for ${REPO_NAME} failed: ${result}")
    endif()

    # build and install module
    execute_process(COMMAND ${CMAKE_COMMAND} --build build --config Release -- -j ${N}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME})
    if(result)
        message(FATAL_ERROR "Build step for ${REPO_NAME} failed: ${result}")
    endif()

    execute_process(COMMAND ${CMAKE_COMMAND} --install build
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/${REPO_NAME})
    if(result)
        message(FATAL_ERROR "Install step for ${REPO_NAME} failed: ${result}")
    endif()
endfunction()

add_subdirectory(googletest)
add_subdirectory(yamlcpp)
add_subdirectory(zeromq)