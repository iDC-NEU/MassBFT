//
// Created by peng on 11/7/22.
//

#include "gtest/gtest.h"
#include "ycsb/core/generator/uniform_long_generator.h"

class UniformLongGeneratorTest : public ::testing::Test {
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

TEST_F(UniformLongGeneratorTest, UniformLongCorrectness) {
    int min=10, max=100, count=10000000;
    auto sz = ycsb::core::UniformLongGenerator::NewUniformLongGenerator(min, max);
    auto mapping = TestCount(*sz, count);
    const int size = (int)mapping.size();
    EXPECT_TRUE(size == max-min+1);
    for (int i = min; i <= max; i++) {
        EXPECT_TRUE(mapping[i] < (double)count/size*1.05) << "distribution test failed!";
        EXPECT_TRUE(mapping[i] > (double)count/size*0.95) << "distribution test failed!";
    }
}
