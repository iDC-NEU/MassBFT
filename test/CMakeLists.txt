cmake_minimum_required(VERSION 3.12)

add_subdirectory(lib/googletest)
set(GTEST_INCLUDE_DIR lib/googletest/googletest/include)

add_subdirectory(division)
add_subdirectory(common)