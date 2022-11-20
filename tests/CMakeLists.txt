cmake_minimum_required(VERSION 3.14...3.22)

project(NBPTests LANGUAGES CXX)

# ---- Options ----

option(ENABLE_TEST_COVERAGE "Enable test coverage" OFF)

CPMAddPackage(
        NAME googletest
        GITHUB_REPOSITORY google/googletest
        GIT_TAG release-1.12.1
        VERSION 1.12.1
        OPTIONS "INSTALL_GTEST OFF" "gtest_force_shared_crt"
)

# ---- Create binary ----

file(GLOB_RECURSE sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")
add_executable(${PROJECT_NAME} ${sources})
target_link_libraries(${PROJECT_NAME} gtest gtest_main gmock NBP ${COMMON_DEPENDENCIES})
set_target_properties(${PROJECT_NAME} PROPERTIES CXX_STANDARD 20)

# enable compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  target_compile_options(NBP PUBLIC -Wall -Wpedantic -Wextra -Werror)
elseif(MSVC)
  target_compile_options(NBP PUBLIC /W4 /WX)
  target_compile_definitions(${PROJECT_NAME} PUBLIC DOCTEST_CONFIG_USE_STD_HEADERS)
endif()

# ---- Add GreeterTests ----

enable_testing()
add_test(${PROJECT_NAME} ${PROJECT_NAME})

# ---- code coverage ----

if(ENABLE_TEST_COVERAGE)
  target_compile_options(NBP PUBLIC -O0 -g -fprofile-arcs -ftest-coverage)
  target_link_options(NBP PUBLIC -fprofile-arcs -ftest-coverage)
endif()
