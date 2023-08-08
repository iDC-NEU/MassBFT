cmake_minimum_required(VERSION 3.14...3.22)

### HTTP support
find_package(cpp-httplib_install
        CONFIG
        REQUIRED
        PATHS ${CMAKE_SOURCE_DIR}/vendor
        )

file(GLOB_RECURSE pension_headers CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
file(GLOB_RECURSE pension_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

add_executable(${PROJECT_NAME}_pension ${pension_headers} ${pension_sources})

set_target_properties(${PROJECT_NAME}_pension PROPERTIES CXX_STANDARD 20 OUTPUT_NAME "pension")

target_link_libraries(${PROJECT_NAME}_pension NBP cxxopts ${COMMON_DEPENDENCIES})
