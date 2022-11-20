cmake_minimum_required(VERSION 3.14...3.22)

project(NBPStandalone LANGUAGES CXX)

# --- Import tools ----

include(../cmake/tools.cmake)

# ---- Dependencies ----

include(../cmake/CPM.cmake)

CPMAddPackage(
  GITHUB_REPOSITORY jarro2783/cxxopts
  VERSION 3.0.0
  OPTIONS "CXXOPTS_BUILD_EXAMPLES NO" "CXXOPTS_BUILD_TESTS NO" "CXXOPTS_ENABLE_INSTALL YES"
)

# ---- Create standalone executable ----

file(GLOB sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ycsb/*.cpp)

add_executable(${PROJECT_NAME}_ycsb ${sources})

set_target_properties(${PROJECT_NAME}_ycsb PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "NBP_ycsb")

target_link_libraries(${PROJECT_NAME}_ycsb NBP cxxopts)
