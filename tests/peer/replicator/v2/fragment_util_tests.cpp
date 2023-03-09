//
// Created by user on 23-3-9.
//


#include "peer/replicator/v2/fragment_util.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class FragmentUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    void startTesting() {
        for (int i=0, j=0; i<(int)sizeVector.size(); i++) {
            int localId = localIdVector[i];
            auto ret = peer::v2::FragmentUtil::CalculateFragmentConfig(localServerCount, remoteServerCount, localId);
            ASSERT_TRUE((int)ret.size() == sizeVector[i]) << localIdVector[i] << " failed!";
            for (const auto& it: ret) {
                if ((int)resultExpectedList.size() <= j) {
                    CHECK(false) << "too many result!";
                }
                ASSERT_TRUE(it == resultExpectedList[j]) << localIdVector[i] << " failed!";
                j++;
            }
        }
    }

protected:
    int localServerCount{};
    int remoteServerCount{};
    std::vector<int> sizeVector;
    std::vector<int> localIdVector;
    std::vector<std::pair<int, int>> resultExpectedList;
};

TEST_F(FragmentUtilTest, TestLCM) {
    ASSERT_TRUE(peer::v2::FragmentUtil::LCM(78, 52) == 156);
    ASSERT_TRUE(peer::v2::FragmentUtil::LCM(12, 18) == 36);
    ASSERT_TRUE(peer::v2::FragmentUtil::LCM(150, 200) == 600);
    ASSERT_TRUE(peer::v2::FragmentUtil::LCM(8, 9) == 72);
}

TEST_F(FragmentUtilTest, TestCalculateFragmentConfigSimple) {
    localServerCount = 4;
    remoteServerCount = 8;
    sizeVector = {2, 2, 2, 2};
    localIdVector = {0, 1, 2, 3};
    resultExpectedList = {
            {0, 1},
            {1, 2},
            {2, 3},
            {3, 4},
            {4, 5},
            {5, 6},
            {6, 7},
            {7, 8},
    };
    startTesting();
}

TEST_F(FragmentUtilTest, TestCalculateFragmentConfigNotEqual) {
    localServerCount = 4;
    remoteServerCount = 7;
    sizeVector = {2, 3, 3, 2};
    localIdVector = {0, 1, 2, 3};
    resultExpectedList = {
            {0, 4},
            {4, 7},
            {7, 8},
            {8, 12},
            {12, 14},
            {14, 16},
            {16, 20},
            {20, 21},
            {21, 24},
            {24, 28},
    };
    startTesting();
}

TEST_F(FragmentUtilTest, TestGetBFGConfig) {
    auto ret = peer::v2::FragmentUtil::GetBFGConfig(7, 4);
    ASSERT_TRUE( ret.dataShardCnt == 9);
    ASSERT_TRUE( ret.parityShardCnt == 19);
    ret = peer::v2::FragmentUtil::GetBFGConfig(601, 301);
    ASSERT_TRUE( ret.dataShardCnt == 60300);
    ASSERT_TRUE( ret.parityShardCnt == 120601);
}
