cmake_minimum_required(VERSION 3.12)

# Set the project name
project (unit_tests)

# enable CTest testing
enable_testing()

add_subdirectory(googletest)
add_subdirectory(division)
add_subdirectory(common)
