//
// Created by user on 23-3-20.
//

#include "peer/replicator/v2/zmq_port_util.h"
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
    util::Matrix2D<std::unique_ptr<peer::v2::ZMQPortUtil>> matrix(3, 6);
    for (const auto& it: regionsNodeCount) {
        for (int i=0; i<it.second; i++) {
            matrix(it.first, i) = std::make_unique<peer::v2::ZMQPortUtil>(regionsNodeCount, it.first, i, portOffset);
        }
    }
    ASSERT_TRUE(matrix(0, 0)->getFRServerPort(0) == 0);
    ASSERT_TRUE(matrix(0, 0)->getRFRServerPort(0) == 3);
    ASSERT_TRUE(matrix(0, 1)->getFRServerPort(0) == 6);
    ASSERT_TRUE(matrix(0, 1)->getRFRServerPort(0) == 9);
    ASSERT_TRUE(matrix(1, 0)->getFRServerPort(0) == 24);
    ASSERT_TRUE(matrix(1, 0)->getRFRServerPort(0) == 27);
    ASSERT_TRUE(matrix(2, 0)->getFRServerPort(0) == 54);
    ASSERT_TRUE(matrix(2, 0)->getRFRServerPort(0) == 57);
    ASSERT_TRUE(matrix(2, 5)->getFRServerPort(2) == 86);
    ASSERT_TRUE(matrix(2, 5)->getRFRServerPort(2) == 89);

}