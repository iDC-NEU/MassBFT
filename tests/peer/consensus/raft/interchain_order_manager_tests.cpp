//
// Created by user on 23-4-5.
//

#include "peer/consensus/raft/interchain_order_manager.h"
#include "common/timer.h"
#include "common/thread_pool_light.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class OrderManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        iom = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
        queue = {};
        iom->setSubChainIds({0, 1, 2});   // 3 regions tests
        iom->setDeliverCallback([&](const peer::consensus::v2::InterChainOrderManager::Cell* cell) {
            LOG(INFO) << "ChainNumber: " << cell->subChainId << ", BlockNumber: " << cell->blockNumber;
            queue.push(cell);
        });
    };

    void TearDown() override {
    };

    std::unique_ptr<peer::consensus::v2::InterChainOrderManager> iom;
    std::queue<const peer::consensus::v2::InterChainOrderManager::Cell*> queue;
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
        ASSERT_TRUE(iom->pushDecision(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
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
        ASSERT_TRUE(iom->pushDecision(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
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
        ASSERT_TRUE(iom->pushDecision(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 4);
}

TEST_F(OrderManagerTest, TestCoverage) {
    std::vector<std::tuple<int, int, std::unordered_map<int, int>>> input {
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 0
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 1
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 2
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
            {0, 1, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, 1}, {1, -1}, {2, -1}}},  // node 1
            {1, 0, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, -1}, {1, -1}, {2, -1}}},
            {1, 1, {{0, 1}, {1, 0}, {2, -1}}},  // node 1
            {1, 1, {{0, 0}, {1, 0}, {2, -1}}},
            {1, 1, {{0, 0}, {1, 0}, {2, -1}}},
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
    };
    for (const auto& it: input) {
        ASSERT_TRUE(iom->pushDecision(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 4);
}

TEST_F(OrderManagerTest, TestDeterminsticOrder) {
    std::vector<std::tuple<int, int, std::unordered_map<int, int>>> input {
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 0
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 1
            {0, 0, {{0, -1}, {1, -1}, {2, -1}}},    // region 2

            {2, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {2, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {2, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {2, 1, {{0, 1}, {1, -1}, {2, 0}}},
            {2, 1, {{0, 1}, {1, -1}, {2, 0}}},
            {2, 1, {{0, 1}, {1, -1}, {2, 0}}},

            {1, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {1, 0, {{0, 1}, {1, -1}, {2, -1}}},
            {1, 1, {{0, 1}, {1, 0}, {2, -1}}},
            {1, 1, {{0, 1}, {1, 0}, {2, -1}}},
            {1, 1, {{0, 1}, {1, 0}, {2, -1}}},

            {0, 1, {{0, -1}, {1, -1}, {2, -1}}},
            {0, 1, {{0, -1}, {1, -1}, {2, -1}}},
            {0, 1, {{0, 0}, {1, -1}, {2, -1}}},
    };
    for (const auto& it: input) {
        ASSERT_TRUE(iom->pushDecision(std::get<0>(it), std::get<1>(it), std::get<2>(it)));
    }
    ASSERT_TRUE(queue.size() == 6);
}

using Cell = peer::consensus::v2::InterChainOrderManager::Cell;

TEST_F(OrderManagerTest, TestDeterminsticOrder2) {
    std::vector<std::unique_ptr<peer::consensus::v2::OrderAssigner>> oiList;
    oiList.reserve(3);
    for (int i=0; i<3; i++) {
        auto ret = std::make_unique<peer::consensus::v2::OrderAssigner>();
        ret->setSubChainIds({0, 1, 2});
        oiList.push_back(std::move(ret));
    }
    util::thread_pool_light tp;
    const Cell* lastCell = nullptr;
    iom->setDeliverCallback([&](const Cell* cell) {
        // LOG(INFO) << "RESULT"; cell->printDebugString();
        if (lastCell != nullptr) {
            if(!lastCell->operator<(cell)) {
                CHECK(false);
            }
        }
        lastCell = cell;
    });

    auto func2 = [&](int id) {
        for (int i=0; i< 10000; i++) {
            for (int j=0; j<3; j++) {
                auto vc = oiList[j]->getBlockOrder(id, i);
                iom->pushDecision(id, i, std::move(vc));
                // util::Timer::sleep_ms(15 - rand()%5 - id*2);
            }
        }
    };
    std::thread sender_1(func2, 0);
    std::thread sender_2(func2, 1);
    std::thread sender_3(func2, 2);

    sender_1.join();
    sender_2.join();
    sender_3.join();
}
