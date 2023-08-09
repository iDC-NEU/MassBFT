//
// Created by peng on 11/7/22.
//

#include "client/core/generator/scrambled_zipfian_generator.h"
#include "gtest/gtest.h"

class ScrambledZipfianGeneratorTest : public ::testing::Test {
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

TEST_F(ScrambledZipfianGeneratorTest, ScrambledZipfianCorrectness) {
    auto target = {848536, 1166912, 793859, 859141, 993748, 949876, 864642, 876079, 932109, 792692, 922406};
    int min=0, max=10, count=10000000;
    auto sz = client::core::ScrambledZipfianGenerator::NewScrambledZipfianGenerator(min, max);
    auto mapping = TestCount(*sz, count);
    EXPECT_TRUE(mapping.size() == 11);
    for (uint64_t i = 0; i <= 10; i++) {
        EXPECT_TRUE(mapping[i] < target.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(mapping[i] > target.begin()[i]*0.95) << "distribution test failed!";
    }
}
