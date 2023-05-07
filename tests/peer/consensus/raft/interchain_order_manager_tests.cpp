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
        iom->setSubChainCount(3);   // 3 regions tests
        iom->setDeliverCallback([&](const peer::consensus::v2::InterChainOrderManager::Cell* cell) {
            LOG(INFO) << "ChainNumber: " << cell->subChainId << ", BlockNumber: " << cell->blockNumber;
            queue.push(cell);
        });
    };

    void TearDown() override {
    };

    void pushHelper(int subChainId, int blockNumber, const std::vector<int>& decision) {
        for (int i=0; i<(int)decision.size(); i++) {
            ASSERT_TRUE(iom->pushDecision(subChainId, blockNumber, {i, decision[i]}));
        }
    }

    std::unique_ptr<peer::consensus::v2::InterChainOrderManager> iom;
    std::queue<const peer::consensus::v2::InterChainOrderManager::Cell*> queue;
};

TEST_F(OrderManagerTest, BasicTest1) {  // block 0,0 must pop
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {0, 0, {-1, -1, -1}}
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    ASSERT_TRUE(queue.size() == 1);
    auto* cell = queue.front();
    ASSERT_TRUE(cell->subChainId == 0);
    ASSERT_TRUE(cell->blockNumber == 0);
}

TEST_F(OrderManagerTest, BasicTest2) {  // block 1,0 must wait
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {1, 0, {-1, -1, -1}}
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    ASSERT_TRUE(queue.empty());
}

TEST_F(OrderManagerTest, BasicTest3) {  // block 0,1 -> must also pop
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {0, 0, {-1, -1, -1}},
            {1, 0, {0, -1, -1}},
            {0, 1, {0, -1, -1}},
            {2, 0, {1, -1, -1}},
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    iom->printBuffer();
    ASSERT_TRUE(!queue.empty());
    queue.pop();
    auto* cell = queue.back();
    ASSERT_TRUE(cell->subChainId == 1);
    ASSERT_TRUE(cell->blockNumber == 0);
}

TEST_F(OrderManagerTest, BasicTest4) {  // block 1,1 -> must wait
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {0, 0, {-1, -1, -1}},
            {1, 0, {-1, -1, -1}},
            {2, 0, {-1, -1, -1}},
            {1, 1, {-1, 0, -1}},
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    iom->printBuffer();
    ASSERT_TRUE(!queue.empty());
    queue.pop();
    auto* cell = queue.back();
    ASSERT_TRUE(cell->subChainId == 2);
    ASSERT_TRUE(cell->blockNumber == 0);
}

TEST_F(OrderManagerTest, OutOfOrder) {  // BasicTest3
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {1, 0, {0, -1, -1}},
            {2, 0, {1, -1, -1}},
            {0, 0, {-1, -1, -1}},
            {0, 1, {0, -1, -1}},
            // blocks within a sub chain must in order
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    iom->printBuffer();
    ASSERT_TRUE(!queue.empty());
    queue.pop();
    auto* cell = queue.back();
    ASSERT_TRUE(cell->subChainId == 1);
    ASSERT_TRUE(cell->blockNumber == 0);
}

TEST_F(OrderManagerTest, TestUnBalanced1) { // #2 is slowest (5, 3, 1)
    std::vector<std::tuple<int, int, std::vector<int>>> input {
            {0, 0, {-1, -1, -1}},
            {1, 0, {-1, -1, -1}},
            {2, 0, {4, 2, -1}},     // (2, 0) must know that (0, 4) and (1, 2) is finished

            {0, 1, {0, -1, -1}},
            {1, 1, {-1, 0, -1}},

            {0, 2, {1, -1, -1}},
            {1, 2, {4, 1, -1}},    // (1, 2) must know that (0, 4) is finished

            {0, 3, {2, -1, -1}},

            {0, 4, {3, -1, -1}},
    };
    for (const auto& it: input) {
        pushHelper(std::get<0>(it), std::get<1>(it), std::get<2>(it));
    }
    iom->printBuffer();
    ASSERT_TRUE(!queue.empty());
    queue.pop();
    auto* cell = queue.back();
    ASSERT_TRUE(cell->subChainId == 0);
    ASSERT_TRUE(cell->blockNumber == 4);
    // ChainNumber: 0, BlockNumber: 0
    // ChainNumber: 1, BlockNumber: 0
    // ChainNumber: 1, BlockNumber: 1
    // ChainNumber: 0, BlockNumber: 1
    // ChainNumber: 0, BlockNumber: 2
    // ChainNumber: 0, BlockNumber: 3
    // ChainNumber: 0, BlockNumber: 4
    // ---- buffer----
    // 1 2, weight:{4, 1, -1, }
    // 2 0, weight:{4, 2, -1, }
}

using Cell = peer::consensus::v2::InterChainOrderManager::Cell;

TEST_F(OrderManagerTest, TestDeterminsticOrder2) {
    std::vector<std::unique_ptr<peer::consensus::v2::OrderAssigner>> oiList;
    oiList.reserve(3);
    for (int i=0; i<3; i++) {
        auto ret = std::make_unique<peer::consensus::v2::OrderAssigner>();
        ret->setLocalChainId(i);
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
                std::this_thread::yield();
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

TEST_F(OrderManagerTest, TestDeterminsticOrder3) {
    std::vector<std::unique_ptr<peer::consensus::v2::OrderAssigner>> oiList;
    oiList.reserve(3);
    for (int i=0; i<3; i++) {
        auto ret = std::make_unique<peer::consensus::v2::OrderAssigner>();
        ret->setLocalChainId(i);
        oiList.push_back(std::move(ret));
    }
    auto iom_1 = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
    auto iom_2 = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
    iom_1->setSubChainCount(3);
    iom_2->setSubChainCount(3);
    std::vector<const Cell*> resultList_1;
    std::vector<const Cell*> resultList_2;

    util::thread_pool_light tp;
    const Cell* lastCell_1 = nullptr;
    iom_1->setDeliverCallback([&](const Cell* cell) {
        if (lastCell_1 != nullptr) {
            if(!lastCell_1->operator<(cell)) {
                CHECK(false);
            }
        }
        lastCell_1 = cell;
        resultList_1.push_back(cell);
    });
    const Cell* lastCell_2 = nullptr;
    iom_2->setDeliverCallback([&](const Cell* cell) {
        if (lastCell_2 != nullptr) {
            if(!lastCell_2->operator<(cell)) {
                CHECK(false);
            }
        }
        lastCell_2 = cell;
        resultList_2.push_back(cell);
    });

    auto func2 = [&](int id) {
        for (int i=0; i< 10000; i++) {
            for (int j=0; j<3; j++) {
                auto vc = oiList[j]->getBlockOrder(id, i);
                iom_1->pushDecision(id, i, vc);
                std::this_thread::yield();
                iom_2->pushDecision(id, i, vc);
                std::this_thread::yield();
            }
        }
    };
    std::thread sender_1(func2, 0);
    std::thread sender_2(func2, 1);
    std::thread sender_3(func2, 2);

    sender_1.join();
    sender_2.join();
    sender_3.join();

    ASSERT_TRUE(resultList_1.size() == resultList_2.size());
    bool printFlag = false;
    int count=0;
    for (int i=0; i<(int)resultList_1.size(); i++) {
        if(resultList_1[i]->subChainId != resultList_2[i]->subChainId || resultList_1[i]->blockNumber != resultList_2[i]->blockNumber) {
            if (!printFlag) {
                resultList_1[i-1]->printDebugString();
                resultList_2[i-1]->printDebugString();
            }
            printFlag = true;
        }
        if (printFlag) {
            resultList_1[i]->printDebugString();
            CHECK(resultList_1[i-1]->operator<(resultList_1[i]));
            resultList_2[i]->printDebugString();
            CHECK(resultList_2[i-1]->operator<(resultList_2[i]));
            count++;
        }
        if (count == 100) {
            CHECK(false);
        }
    }
    CHECK(!printFlag);
}
