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
        peer::v2::FragmentUtil fu(localServerCount, remoteServerCount);
        for (int i=0, j=0; i<(int)sizeVector.size(); i++) {
            int localId = localIdVector[i];
            auto ret = fu.getSenderConfig(localId);
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
    std::vector<peer::v2::FragmentUtil::FragmentConfig> resultExpectedList;
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
            {0, 1, 0, 0},
            {1, 2, 0, 1},
            {2, 3, 1, 2},
            {3, 4, 1, 3},
            {4, 5, 2, 4},
            {5, 6, 2, 5},
            {6, 7, 3, 6},
            {7, 8, 3, 7},
    };
    startTesting();
}

TEST_F(FragmentUtilTest, TestCalculateFragmentConfigNotEqual) {
    localServerCount = 4;
    remoteServerCount = 7;
    sizeVector = {2, 3, 3, 2};
    localIdVector = {0, 1, 2, 3};
    resultExpectedList = {
            {0, 4, 0, 0},
            {4, 7, 0, 1},
            {7, 8, 1, 1},
            {8, 12, 1, 2},
            {12, 14, 1, 3},
            {14, 16, 2, 3},
            {16, 20, 2, 4},
            {20, 21, 2, 5},
            {21, 24, 3, 5},
            {24, 28, 3, 6},
    };
    startTesting();
}

TEST_F(FragmentUtilTest, TestGetBFGConfig) {
    peer::v2::FragmentUtil fu(7, 4);
    auto ret = fu.getBFGConfig();
    ASSERT_TRUE( ret.dataShardCnt == 10);
    ASSERT_TRUE( ret.parityShardCnt == 18);
    fu.reset(4, 4);
    ret = fu.getBFGConfig();
    ASSERT_TRUE( ret.dataShardCnt == 2);
    ASSERT_TRUE( ret.parityShardCnt == 2);
    fu.reset(601, 301);
    ret = fu.getBFGConfig();
    ASSERT_TRUE( ret.dataShardCnt == 60301);
    ASSERT_TRUE( ret.parityShardCnt == 120600);
}
