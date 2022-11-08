# Download and unpack yamlcpp at configure time
configure_file(CMakeLists.txt.in src/yamlcpp-download/CMakeLists.txt)
# Call CMake to download yamlcpp
execute_process(COMMAND ${CMAKE_COMMAND} .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp-download)
if(result)
    message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()

execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp-download)
if(result)
    message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()
# now that the repo is copied into ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp
# Call CMake to download
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR} -DBUILD_SHARED_LIBS=OFF -DYAML_CPP_BUILD_TESTS=OFF -DYAML_CPP_BUILD_TOOLS=OFF .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp)
if(result)
    message(FATAL_ERROR "CMake step for yamlcpp failed: ${result}")
endif()

execute_process(COMMAND ${CMAKE_COMMAND} --build .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp)
if(result)
    message(FATAL_ERROR "Build step for yamlcpp failed: ${result}")
endif()

execute_process(COMMAND ${CMAKE_COMMAND} --install .
        RESULT_VARIABLE result
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/src/yamlcpp)
if(result)
    message(FATAL_ERROR "Build step for yamlcpp failed: ${result}")
endif()
