//
// Created by user on 23-3-20.
//

#include "common/zmq_port_util.h"
#include "common/matrix_2d.h"

#include "gtest/gtest.h"

class ZMQPortUtilTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};


TEST_F(ZMQPortUtilTest, IntrgrateTest4_4) {
    std::unordered_map<int, int> regionsNodeCount;
    regionsNodeCount[0] = 4;
    regionsNodeCount[1] = 5;
    regionsNodeCount[2] = 6;

    int portOffset = 0;
    util::Matrix2D<std::unique_ptr<util::ZMQPortUtil>> matrix(3, 6);
    for (const auto& it: regionsNodeCount) {
        for (int i=0; i<it.second; i++) {
            matrix(it.first, i) = std::make_unique<util::SingleServerZMQPortUtil>(regionsNodeCount, it.first, i, portOffset);
        }
    }
    int offset = 75;
    ASSERT_TRUE(matrix(0, 0)->getFRServerPort(0) == offset + 0);
    ASSERT_TRUE(matrix(0, 0)->getRFRServerPort(0) == offset + 3);
    ASSERT_TRUE(matrix(0, 1)->getFRServerPort(0) == offset + 6);
    ASSERT_TRUE(matrix(0, 1)->getRFRServerPort(0) == offset + 9);
    ASSERT_TRUE(matrix(1, 0)->getFRServerPort(0) == offset + 24);
    ASSERT_TRUE(matrix(1, 0)->getRFRServerPort(0) == offset + 27);
    ASSERT_TRUE(matrix(2, 0)->getFRServerPort(0) == offset + 54);
    ASSERT_TRUE(matrix(2, 0)->getRFRServerPort(0) == offset + 57);
    ASSERT_TRUE(matrix(2, 5)->getFRServerPort(2) == offset + 86);
    ASSERT_TRUE(matrix(2, 5)->getRFRServerPort(2) == offset + 89);

}