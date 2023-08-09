//
// Created by peng on 11/7/22.
//

#include "client/core/generator/acknowledge_counter_generator.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include <queue>

class AcknowledgeCounterGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    template<class T>
    static auto TestCount(T& sz, uint64_t count) {
        std::unordered_map<uint64_t, int> countVec;
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = sz.nextValue();
            countVec[rnd]++; // offset
        }
        return countVec;
    }

};

// Test that advancing past {@link Integer#MAX_VALUE} works.
TEST_F(AcknowledgeCounterGeneratorTest, AcknowledgeCounterGeneratorCorrectness) {
    /** The size of the window of pending id ack's. 2^20 = {@value} */
    static const int WINDOW_SIZE = 1 << 20;

    const auto toTry = WINDOW_SIZE * 3;

    client::core::AcknowledgedCounterGenerator generator(INT64_MAX - 1000);

    std::queue<uint64_t> q;

    for (auto i = 0; i < toTry; ++i) {
        auto value = generator.nextValue();
        q.push(value);
        while (q.size() >= 1000) {
            auto first = q.front();
            q.pop();
            // Don't always advance by one.
            if (util::Timer::time_now_ns()%2 == 0) {
                generator.acknowledge((int)first);
            } else {
                auto second = q.front();
                q.pop();
                q.push(first);
                generator.acknowledge((int)second);
            }
        }
    }
}
