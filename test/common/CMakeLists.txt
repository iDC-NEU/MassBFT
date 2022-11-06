cmake_minimum_required(VERSION 3.12)
project(common_tests)

# include ALL the tests
set(SOURCE_FILES main.cpp common_tests.cpp)

add_executable(${PROJECT_NAME} ${SOURCE_FILES})

# link ALL the required libraries
target_link_libraries(${PROJECT_NAME}
        gtest
        )

# include google test header
target_include_directories(${PROJECT_NAME}
        PRIVATE
        ${GTEST_INCLUDE_DIR}
        )

# install if required
install(TARGETS ${PROJECT_NAME} DESTINATION bin)
