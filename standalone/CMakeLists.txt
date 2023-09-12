cmake_minimum_required(VERSION 3.14...3.22)

project(NBPStandalone LANGUAGES CXX)

CPMAddPackage(
        GITHUB_REPOSITORY jarro2783/cxxopts
        VERSION 3.0.0
        OPTIONS "CXXOPTS_BUILD_EXAMPLES NO" "CXXOPTS_BUILD_TESTS NO" "CXXOPTS_ENABLE_INSTALL YES"
)

# ---- Create peer standalone executable ----

file(GLOB peer_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/peer/*.cpp)

add_executable(${PROJECT_NAME}_peer ${peer_sources})

set_target_properties(${PROJECT_NAME}_peer PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "peer")

target_link_libraries(${PROJECT_NAME}_peer NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create ca standalone executable ----

file(GLOB ca_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/ca/*.cpp)

add_executable(${PROJECT_NAME}_ca ${ca_sources})

set_target_properties(${PROJECT_NAME}_ca PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "ca")

target_link_libraries(${PROJECT_NAME}_ca NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create ycsb standalone executable ----

file(GLOB ycsb_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/client/ycsb/*.cpp)

add_executable(${PROJECT_NAME}_ycsb ${ycsb_sources})

set_target_properties(${PROJECT_NAME}_ycsb PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "ycsb")

target_link_libraries(${PROJECT_NAME}_ycsb NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create tpcc standalone executable ----

file(GLOB tpcc_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/client/tpcc/*.cpp)

add_executable(${PROJECT_NAME}_tpcc ${tpcc_sources})

set_target_properties(${PROJECT_NAME}_tpcc PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "tpcc")

target_link_libraries(${PROJECT_NAME}_tpcc NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create small_bank standalone executable ----

file(GLOB small_bank_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/client/small_bank/*.cpp)

add_executable(${PROJECT_NAME}_small_bank ${small_bank_sources})

set_target_properties(${PROJECT_NAME}_small_bank PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "small_bank")

target_link_libraries(${PROJECT_NAME}_small_bank NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create crdt standalone executable ----

file(GLOB crdt_sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/client/crdt/*.cpp)

add_executable(${PROJECT_NAME}_crdt ${crdt_sources})

set_target_properties(${PROJECT_NAME}_crdt PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "crdt")

target_link_libraries(${PROJECT_NAME}_crdt NBP cxxopts ${COMMON_DEPENDENCIES})

# ---- Create pension standalone executable ----

file(GLOB pension_headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/client/pension_demo/*.h")
file(GLOB pension_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/client/pension_demo/*.cpp")

add_executable(${PROJECT_NAME}_pension ${pension_headers} ${pension_sources})

set_target_properties(${PROJECT_NAME}_pension PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "pension")

target_link_libraries(${PROJECT_NAME}_pension NBP cxxopts ${COMMON_DEPENDENCIES})
