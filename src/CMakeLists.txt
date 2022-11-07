cmake_minimum_required(VERSION 3.12)
project(divider)

# This project depend on some sub project
add_subdirectory(division)
add_subdirectory(ycsb)

# After source files is built.
set(SOURCE_FILES main.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})
target_link_libraries(${PROJECT_NAME} division)

# install to ${CMAKE_INSTALL_PREFIX}/bin folder.
install(TARGETS divider DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
