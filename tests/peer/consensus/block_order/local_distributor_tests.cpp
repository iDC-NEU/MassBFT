//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/local_distributor.h"
#include "common/timer.h"
#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

using namespace peer::consensus::v2;

class LocalDistributorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(LocalDistributorTest, BasicTest) {
    // region 0 has 4 nodes
    int nodeCount = 4;
    auto region0 = tests::ProtoBlockUtils::GenerateNodesConfig(0, nodeCount, 0);
    std::vector<std::unique_ptr<LocalDistributor>> dList(nodeCount);
    // receive message list
    std::mutex deliverMutex;
    std::vector<std::string> receiveMsgList;
    CHECK(dList.size() == region0.size());
    for (int i=0; i<(int)dList.size(); i++) {
        dList[i] = LocalDistributor::NewLocalDistributor(region0, i);
        CHECK(dList[i] != nullptr) << "init failed!";
        dList[i]->setDeliverCallback([&](std::string msg) {
            std::unique_lock guard(deliverMutex);
            receiveMsgList.push_back(std::move(msg));
        });
    }
    util::Timer::sleep_sec(0.5);    // wait until ready
    // node 0 send a message
    dList[0]->gossip("node0");
    util::Timer::sleep_sec(0.5);    // wait until received
    {
        std::unique_lock guard(deliverMutex);
        ASSERT_TRUE((int)receiveMsgList.size() == nodeCount);
        for (auto& it: receiveMsgList) {
            ASSERT_TRUE(it == "node0");
        }
        receiveMsgList.clear();
    }
    // node 1 send a message
    dList[1]->gossip("node1");
    util::Timer::sleep_sec(0.5);    // wait until received
    {
        std::unique_lock guard(deliverMutex);
        ASSERT_TRUE((int)receiveMsgList.size() == nodeCount);
        for (auto& it: receiveMsgList) {
            ASSERT_TRUE(it == "node1");
        }
        receiveMsgList.clear();
    }
}