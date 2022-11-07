cmake_minimum_required(VERSION 3.12)

project(neuchain_plus)

set(CMAKE_CXX_STANDARD 20)
# Set debug and release flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20 -D__const__= -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_CXX_FLAGS_COMMON}  -g -O0 -fprofile-arcs -ftest-coverage")
SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_CXX_FLAGS_COMMON} -O3")

# CMAKE_INSTALL_PREFIX: Install directory used by install().
# PROJECT_SOURCE_DIR refers to the source directory of the most recent project() command.
set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR})

# include INSTALL_INCLUDE_DIR
include_directories(${PROJECT_SOURCE_DIR}/include)

# set libs name
SET(LIB_PTHREADS pthread)
SET(LIB_GLOG glog)
SET(LIB_YAML yaml-cpp)

SET(LIB_YCSB ycsb)

add_subdirectory(src)
add_subdirectory(test)
