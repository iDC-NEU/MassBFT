cmake_minimum_required(VERSION 3.12)

# include ALL the tests
add_executable(division_tests divider_tests.cpp)

target_link_libraries(division_tests
        division
        GTest::GTest
        GTest::Main
        )

add_test(test_all division_tests)
