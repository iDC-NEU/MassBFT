cmake_minimum_required(VERSION 3.12)

set(SOURCE_FILES
        core/generator/zipfian_generator_tests.cpp
        core/generator/scrambled_zipfian_generator_tests.cpp
        core/generator/uniform_long_generator_tests.cpp
        core/generator/acknowledge_counter_generator_tests.cpp
        core/workload/core_workload_test.cpp
        ycsb_tests.cpp
        )

# include ALL the tests
add_executable(ycsb_tests ${SOURCE_FILES})

target_link_libraries(ycsb_tests
        ${LIB_PTHREADS}
        ${LIB_GLOG}
        ${LIB_YCSB}
        ${LIB_YAML}
        GTest::GTest
        GTest::Main
        )

add_test(test_all ycsb_tests)