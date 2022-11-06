cmake_minimum_required(VERSION 3.12)
project(division C CXX)

set(SOURCE_FILES
    division.cpp
)

add_library(division SHARED STATIC ${SOURCE_FILES})

# Specifies include directories to use when compiling a given target.
# The named <target> must have been created by a command such as add_executable() or add_library()
# and must not be an ALIAS target.
target_include_directories(${PROJECT_NAME}
        PRIVATE
        ${CMAKE_SOURCE_DIR}/include/division
        )

# copy the lib.a to INSTALL_LIB_DIR
install(TARGETS division DESTINATION ${CMAKE_INSTALL_PREFIX}/lib)
