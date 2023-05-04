//
// Created by user on 23-5-4.
//

#include "common/timer.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class TimerTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};


TEST_F(TimerTest, TestTimeoutMs) {
    auto start = util::Timer::time_now_ms();
    // LOG(INFO) << "Start at: " << start;
    for (int i=0; i<10; i++) {
        util::Timer::sleep_ms(12);
    }
    auto end = util::Timer::time_now_ms();
    // LOG(INFO) << "End at: " << end;
    auto span = end-start;
    LOG(INFO) << "TestTimeoutMs Span: " << span;
    ASSERT_TRUE(std::abs(span-12*10) < 6); // %5 percentage error prone
}
