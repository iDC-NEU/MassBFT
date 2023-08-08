cmake_minimum_required(VERSION 3.14...3.22)

project(NBPStandalone LANGUAGES CXX)

CPMAddPackage(
        GITHUB_REPOSITORY jarro2783/cxxopts
        VERSION 3.0.0
        OPTIONS "CXXOPTS_BUILD_EXAMPLES NO" "CXXOPTS_BUILD_TESTS NO" "CXXOPTS_ENABLE_INSTALL YES"
)

# ---- Create ycsb standalone executable ----

file(GLOB ycsb_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/client/ycsb/*.cpp)

add_executable(${PROJECT_NAME}_ycsb ${ycsb_sources})

set_target_properties(${PROJECT_NAME}_ycsb PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "ycsb")

target_link_libraries(${PROJECT_NAME}_ycsb NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create peer standalone executable ----

file(GLOB peer_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/peer/*.cpp)

add_executable(${PROJECT_NAME}_peer ${peer_sources})

set_target_properties(${PROJECT_NAME}_peer PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "peer")

target_link_libraries(${PROJECT_NAME}_peer NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create ca standalone executable ----

file(GLOB ca_headers CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ca/*.h)
file(GLOB ca_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ca/*.cpp)

add_executable(${PROJECT_NAME}_ca ${ca_headers} ${ca_sources})

set_target_properties(${PROJECT_NAME}_ca PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "ca")

target_link_libraries(${PROJECT_NAME}_ca NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create pension standalone executable ----

add_subdirectory(client/pension_demo)
