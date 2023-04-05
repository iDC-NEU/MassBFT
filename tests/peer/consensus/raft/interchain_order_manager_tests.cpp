//
// Created by user on 23-4-5.
//

#include <queue>
#include "peer/consensus/raft/interchain_order_manager.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class OrderManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        iom = std::make_unique<peer::consensus::InterChainOrderManager>();
        queue = {};
        iom->setSubChainIds({0, 1, 2});   // 3 regions tests
        iom->setDeliverCallback([&](const peer::consensus::InterChainOrderManager::Cell* cell) {
            queue.push(cell);
        });
    };

    void TearDown() override {
    };

    std::unique_ptr<peer::consensus::InterChainOrderManager> iom;
    std::queue<const peer::consensus::InterChainOrderManager::Cell*> queue;
};

TEST_F(OrderManagerTest, TestOneRegionSequenalPush) {
    std::vector<std::tuple<int, int, std::unordered_map<int, int>>> input {
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 0
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 1
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 2
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}}
    };
    for (const auto& it: input) {
        ASSERT_TRUE(iom->pushBlockWithOrder(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 2);
}

TEST_F(OrderManagerTest, TestOneRegionRandomPush) {
    std::vector<std::tuple<int, int, std::unordered_map<int, int>>> input {
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 0
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 1
            {0, 2, {{0, 1}, {1, -1}, {2, -1}}},
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 2
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 2, {{0, 1}, {1, -1}, {2, -1}}},
            {0, 2, {{0, 1}, {1, -1}, {2, -1}}}
    };
    for (const auto& it: input) {
        ASSERT_TRUE(iom->pushBlockWithOrder(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 3);
}

TEST_F(OrderManagerTest, TestTwoRegionsSequenalPush) {
    std::vector<std::tuple<int, int, std::unordered_map<int, int>>> input {
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 0
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 1
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 2
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 1, {{0, -1}, {1, -1}, {2, -1}}},
            {0, 1, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, 1}, {1, -1}, {2, -1}}},  // node 1
            {1, 0, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 1, {{0, 1}, {1, 0}, {2, -1}}},  // node 1
            {1, 1, {{0, 0}, {1, 0}, {2, -1}}},
            {1, 1, {{0, 0}, {1, 0}, {2, -1}}}
    };
    for (const auto& it: input) {
        ASSERT_TRUE(iom->pushBlockWithOrder(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 4);
}