cmake_minimum_required(VERSION 3.14...3.22)

project(NBPStandalone LANGUAGES CXX)

CPMAddPackage(
        GITHUB_REPOSITORY jarro2783/cxxopts
        VERSION 3.0.0
        OPTIONS "CXXOPTS_BUILD_EXAMPLES NO" "CXXOPTS_BUILD_TESTS NO" "CXXOPTS_ENABLE_INSTALL YES"
)

# ---- Create ycsb standalone executable ----

file(GLOB ycsb_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ycsb/*.cpp)

add_executable(${PROJECT_NAME}_ycsb ${ycsb_sources})

set_target_properties(${PROJECT_NAME}_ycsb PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "NBP_ycsb")

target_link_libraries(${PROJECT_NAME}_ycsb NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create peer standalone executable ----

file(GLOB peer_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/peer/*.cpp)

add_executable(${PROJECT_NAME}_peer ${peer_sources})

set_target_properties(${PROJECT_NAME}_peer PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "NBP_peer")

target_link_libraries(${PROJECT_NAME}_peer NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create initializer standalone executable ----

file(GLOB initializer_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/initializer/*.cpp)

add_executable(${PROJECT_NAME}_initializer ${initializer_sources})

set_target_properties(${PROJECT_NAME}_initializer PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "NBP_initializer")

target_link_libraries(${PROJECT_NAME}_initializer NBP cxxopts ${COMMON_DEPENDENCIES})