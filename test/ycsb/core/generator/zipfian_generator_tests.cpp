//
// Created by peng on 11/6/22.
//
#include <thread>
#include "gtest/gtest.h"
#include "ycsb/core/generator/zipfian_generator.h"

class ZipfianGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static void TestOutOfRange(ycsb::core::ZipfianGenerator& z, uint64_t min, uint64_t max, uint64_t count) {
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = z.nextValue();
            EXPECT_TRUE(min <= rnd);
            EXPECT_TRUE(max >= rnd);
        }
    }

    static auto TestCount(ycsb::core::ZipfianGenerator& z, uint64_t min, uint64_t max, uint64_t count) {
        std::vector<int> countVec(max-min+1);
        for (uint64_t i = 0; i < count; i++) {
            auto rnd = z.nextValue();
            countVec[rnd-min]++; // offset
        }
        return countVec;
    }
    static void TestUniformDistribution() {
        int min=0, max=10, count=10000000;
        ycsb::core::ZipfianGenerator z(min, max, 0);
        auto vec = TestCount(z, min, max, count);
        for (uint64_t i = 0; i < vec.size(); i++) {
            EXPECT_TRUE(vec[i] < (double)count/vec.size()*1.05) << "distribution test failed!";
            EXPECT_TRUE(vec[i] > (double)count/vec.size()*0.95) << "distribution test failed!";
        }
    }
    // the zipf distribution tested from official ycsb
    static std::initializer_list<int> YCSB_ZIPF99_0_10_1E7;
    static std::initializer_list<int> YCSB_ZIPF99_0_1E6_1E7;

protected:

};

TEST_F(ZipfianGeneratorTest, OutOfRange) {
    int min=0, max=10, count=1000;

    auto z = ycsb::core::ZipfianGenerator::NewZipfianGenerator(min, max);
    TestOutOfRange(*z, min, max, count);
}

TEST_F(ZipfianGeneratorTest, ZipfianCorrectness) {
    int min=0, max=10, count=10000000;
    auto z = ycsb::core::ZipfianGenerator::NewZipfianGenerator(min, max);
    auto vec = TestCount(*z, min, max, count);
    EXPECT_TRUE(vec.size() == 11);
    for (uint64_t i = 0; i < 11; i++) {
        EXPECT_TRUE(vec[i] < ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(vec[i] > ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7.begin()[i]*0.95) << "distribution test failed!";
    }
}

TEST_F(ZipfianGeneratorTest, ZipfianCorrectness2) {
    int min=0, max=1000000, count=10000000;
    auto z = ycsb::core::ZipfianGenerator::NewZipfianGenerator(min, max);
    auto vec = TestCount(*z, min, max, count);
    EXPECT_TRUE(vec.size() > 11);
    for (uint64_t i = 0; i < 11; i++) {
        EXPECT_TRUE(vec[i] < ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7.begin()[i]*1.05) << "distribution test failed!";
        EXPECT_TRUE(vec[i] > ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7.begin()[i]*0.95) << "distribution test failed!";
    }
}

TEST_F(ZipfianGeneratorTest, UniformDistribution) {
    TestUniformDistribution();
}

TEST_F(ZipfianGeneratorTest, ConcurrentAccess) {
    std::thread t1(&ZipfianGeneratorTest::TestUniformDistribution);
    std::thread t2(&ZipfianGeneratorTest::TestUniformDistribution);
    std::thread t3(&ZipfianGeneratorTest::TestUniformDistribution);
    t1.join();
    t2.join();
    t3.join();
}

std::initializer_list<int> ZipfianGeneratorTest::YCSB_ZIPF99_0_10_1E7 = {3280393, 1649922, 1198018, 852880, 664195, 542254, 460741, 397192, 352452, 315486, 285257};

std::initializer_list<int> ZipfianGeneratorTest::YCSB_ZIPF99_0_1E6_1E7 = {648903, 327593, 261379, 186162, 144515, 118535, 100134, 87280, 77253, 68863, 62118};
