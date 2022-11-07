cmake_minimum_required(VERSION 3.12)

# include ALL the tests
add_executable(common_tests thread_local_store_tests.cpp)

target_link_libraries(common_tests
        GTest::GTest
        GTest::Main
        )

add_test(test_all common_tests)
