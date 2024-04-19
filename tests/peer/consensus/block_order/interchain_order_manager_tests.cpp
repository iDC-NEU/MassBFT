//
// Created by user on 23-4-5.
//

#include "peer/consensus/block_order/interchain_order_manager.h"
#include "common/timer.h"
#include "common/thread_pool_light.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class OrderManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        iom = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
        queue = {};
        iom->setGroupCount(3);   // 3 regions tests
        iom->setDeliverCallback([&](const peer::consensus::v2::InterChainOrderManager::Cell* cell) {
            LOG(INFO) << "ChainNumber: " << cell->groupId << ", BlockNumber: " << cell->blockId;
            queue.push(cell);
        });
    };

    void TearDown() override {
    };

    void pushHelper(int groupId, int blockId, const std::vector<int>& decision) {
        for (int i=0; i<(int)decision.size(); i++) {
            iom->pushDecision(groupId, blockId, i, decision[i]);
        }
    }

    struct Vote {
        int groupId;
        int blockId;
        int voteGroupId;
        int voteWatermark;
    };

    void pushHelper(const std::vector<Vote>& decision) {
        for (const auto & it : decision) {
            LOG(INFO) << it.voteGroupId << " vote " << it.voteWatermark << " to block (" << it.groupId << "," << it.blockId << ")";
            iom->pushDecision(it.groupId, it.blockId, it.voteGroupId, it.voteWatermark);
        }
    }

    bool inOrder(const std::vector<std::pair<int, int>>& blocks) {
        if (blocks.size() != queue.size()) {
            return false;
        }
        int idx = 0;
        while (!queue.empty()) {
            auto* top = queue.front();
            queue.pop();
            if (top->groupId != blocks[idx].first || top->blockId != blocks[idx].second) {
                return false;
            }
            idx++;
        }
        return true;
    }

    std::unique_ptr<peer::consensus::v2::InterChainOrderManager> iom;
    std::queue<const peer::consensus::v2::InterChainOrderManager::Cell*> queue;
};

TEST_F(OrderManagerTest, BasicTest1) {  // block 0,0 must pop
    pushHelper({Vote{0, 0, 1, -1}});
    // pushHelper({Vote{0, 0, 2, -1}}); // not necessary
    // we learned that group 0 increase its local clock to 0
    pushHelper({Vote{1, 0, 0, 0}});
    pushHelper({Vote{2, 0, 0, 0}});
    pushHelper({Vote{2, 0, 1, 0}});
    ASSERT_TRUE(queue.size() == 1);
    auto* cell = queue.front();
    ASSERT_TRUE(cell->groupId == 0);
    ASSERT_TRUE(cell->blockId == 0);
}

TEST_F(OrderManagerTest, BasicTest2) {
    // block 1,0 {-1, 0, -1} must pop
    // reason: block 0,0: 1st bit must be 0, 2nd bit must >=0 (using group 1's consensus instance)
    pushHelper({Vote{1, 0, 0, -1}});
    // we learned that group 1 increase its local clock to 0
    pushHelper({Vote{0, 0, 1, 0}});
    pushHelper({Vote{2, 0, 1, 0}});
    ASSERT_TRUE(queue.empty());
    pushHelper({Vote{2, 0, 0, 0}});
    ASSERT_TRUE(queue.size() == 1);
    auto* cell = queue.front();
    ASSERT_TRUE(cell->groupId == 1);
    ASSERT_TRUE(cell->blockId == 0);
}

TEST_F(OrderManagerTest, BasicTest3) {
    // pushHelper({Vote{0, 0, 0, 0}}); // group 0 vote 0,0 to 0 (preset)
    pushHelper({Vote{0, 0, 1, -1}}); // group 1 vote 0,0 to -1
    // pushHelper({Vote{1, 0, 1, 0}}); // group 1 vote 1,0 to 0 (preset)
    pushHelper({Vote{1, 0, 0, -1}}); // group 0 vote 1,0 to -1
    // pushHelper({Vote{1, 1, 1, 1}}); // group 1 vote 1,1 to 1 (preset)
    // pushHelper({Vote{1, 2, 1, 2}}); // group 1 vote 1,2 to 2 (preset)
    ASSERT_TRUE(queue.empty());
    // we learned that group 0 increase its local clock to 0
    pushHelper({Vote{1, 1, 0, 0}}); // group 0 vote 1,1 to 0
    ASSERT_TRUE(queue.size() == 1); // 0, -1, -1(est) vs. 0(est), -1(est), 0
    pushHelper({Vote{1, 2, 0, 0}}); // group 0 vote 1,2 to 0
    // we learned that group 0 increase its local clock to 1
    pushHelper({Vote{1, 3, 0, 1}});
    ASSERT_TRUE(inOrder({{1, 0}, {0, 0}, {1, 1}, {1, 2}}));
}

TEST_F(OrderManagerTest, BasicTest4) {
    // pushHelper({Vote{0, 0, 0, 0}}); // group 0 vote 0,0 to 0 (preset)
    pushHelper({Vote{1, 0, 0, -1}}); // group 0 vote 1,0 to -1
    // we learned that group 0 increase its local clock to 0
    pushHelper({Vote{1, 1, 0, 0}}); // group 0 vote 1,1 to 0
    pushHelper({Vote{1, 2, 0, 0}}); // group 0 vote 1,2 to 0
    pushHelper({Vote{0, 0, 1, -1}}); // group 1 vote 0,0 to -1
    // pushHelper({Vote{1, 0, 1, 0}}); // group 1 vote 1,0 to 0 (preset)
    // pushHelper({Vote{1, 1, 1, 1}}); // group 1 vote 1,1 to 1 (preset)
    // pushHelper({Vote{1, 2, 1, 2}}); // group 1 vote 1,2 to 2 (preset)
    // we learned that group 1 increase its local clock to 2
    pushHelper({Vote{0, 1, 1, 2}}); // group 1 vote 0,1 to 2
    // we learned that group 0 increase its local clock to 1
    pushHelper({Vote{1, 3, 0, 1}});
    ASSERT_TRUE(inOrder({{1, 0}, {0, 0}, {1, 1}, {1, 2}}));
}

TEST_F(OrderManagerTest, TestUnBalanced1) { // #0 is slow (1:2)
    int round = 10000;
    for (int i=0; i<round; i++) {
        pushHelper({Vote{1, i*2,    1, i*2  }}); // group 1 vote 1,0 to 0 (preset)
        pushHelper({Vote{1, i*2+1,  1, i*2+1}}); // group 1 vote 1,1 to 1 (preset)
        // we learned that group 1 increase its local clock to 1
        pushHelper({Vote{0, i, 1, i*2+1}}); // group 1 vote 0,0 to 1

        // group 1 vote 1,2 to 2 (preset)
        // group 1 vote 1,3 to 3 (preset)
        // we learned that group 1 increase its local clock to 3
        // group 1 vote 0,1 to 3
    }

    for (int i=0; i<round; i++) {
        // we learned that group 0 increase its local clock to -1
        pushHelper({Vote{1, i*2, 0, i-1}}); // group 0 vote 1,0 to -1
        pushHelper({Vote{0, i, 0, i}}); // group 0 vote 0,0 to 0 (preset)
        pushHelper({Vote{1, i*2+1, 0, i-1}}); // group 0 vote 1,1 to -1

        // group 0 vote 1,2 to 0
        // group 0 vote 0,0 to 1 (preset)
        // group 0 vote 1,3 to 0
    }

    std::vector<std::pair<int, int>> blocks;
    for (int i=0; i<round; i++) {
        blocks.emplace_back(1, 2*i);
        blocks.emplace_back(1, 2*i+1);
        blocks.emplace_back(0, i);
    }
    blocks.pop_back();
    blocks.pop_back();
    blocks.pop_back();
    blocks.pop_back();
    ASSERT_TRUE(inOrder(blocks));
}

TEST_F(OrderManagerTest, TestUnBalanced2) { // multi thread
    int round = 10000;

    auto func1 = [&]() {
        for (int i=0; i<round; i++) {
            std::this_thread::yield();
            pushHelper({Vote{1, i*2,    1, i*2  }}); // group 1 vote 1,0 to 0 (preset)
            pushHelper({Vote{1, i*2+1,  1, i*2+1}}); // group 1 vote 1,1 to 1 (preset)
            // we learned that group 1 increase its local clock to 1
            pushHelper({Vote{0, i, 1, i*2+1}}); // group 1 vote 0,0 to 1

            // group 1 vote 1,2 to 2 (preset)
            // group 1 vote 1,3 to 3 (preset)
            // we learned that group 1 increase its local clock to 3
            // group 1 vote 0,1 to 3
        }
    };
    auto func2 = [&]() {
        for (int i=0; i<round; i++) {
            std::this_thread::yield();
            // we learned that group 0 increase its local clock to -1
            pushHelper({Vote{1, i*2, 0, i-1}}); // group 0 vote 1,0 to -1
            pushHelper({Vote{0, i, 0, i}}); // group 0 vote 0,0 to 0 (preset)
            pushHelper({Vote{1, i*2+1, 0, i-1}}); // group 0 vote 1,1 to -1

            // group 0 vote 1,2 to 0
            // group 0 vote 0,0 to 1 (preset)
            // group 0 vote 1,3 to 0
        }
    };

    std::thread sender_2(func2);
    std::thread sender_1(func1);

    sender_2.join();
    sender_1.join();

    std::vector<std::pair<int, int>> blocks;
    for (int i=0; i<round; i++) {
        blocks.emplace_back(1, 2*i);
        blocks.emplace_back(1, 2*i+1);
        blocks.emplace_back(0, i);
    }
    blocks.pop_back();
    blocks.pop_back();
    blocks.pop_back();
    blocks.pop_back();
    ASSERT_TRUE(inOrder(blocks));
}

using Cell = peer::consensus::v2::InterChainOrderManager::Cell;

TEST_F(OrderManagerTest, TestDeterminsticOrder3) {
    auto iom_1 = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
    auto iom_2 = std::make_unique<peer::consensus::v2::InterChainOrderManager>();
    iom_1->setGroupCount(3);
    iom_2->setGroupCount(3);
    std::vector<const Cell*> resultList_1;
    std::vector<const Cell*> resultList_2;

    util::thread_pool_light tp;
    const Cell* lastCell_1 = nullptr;
    iom_1->setDeliverCallback([&](const Cell* cell) {
        LOG(INFO) << "ChainNumber: " << cell->groupId << ", BlockNumber: " << cell->blockId;
        if (lastCell_1 != nullptr) {
            if(!lastCell_1->mustLessThan(cell)) {
                CHECK(false);
            }
        }
        lastCell_1 = cell;
        resultList_1.push_back(cell);
    });
    const Cell* lastCell_2 = nullptr;
    iom_2->setDeliverCallback([&](const Cell* cell) {
        if (lastCell_2 != nullptr) {
            if(!lastCell_2->mustLessThan(cell)) {
                CHECK(false);
            }
        }
        lastCell_2 = cell;
        resultList_2.push_back(cell);
    });

    std::vector<std::unique_ptr<peer::consensus::v2::OrderAssigner>> oiList;
    oiList.reserve(3);
    for (int i=0; i<3; i++) {
        auto ret = std::make_unique<peer::consensus::v2::OrderAssigner>();
        ret->setLocalChainId(i);
        oiList.push_back(std::move(ret));
    }

    int round = 10000;
    auto func2 = [&](int id) {
        for (int i=0; i<round; i++) {
            if (i - 10 >= 0) {   // simulate delay
                oiList[id]->increaseLocalClock(id, i - 10);
            }
            for (int j=0; j<3; j++) {
                auto vc = oiList[id]->getBlockOrder(j, i);
                iom_1->pushDecision(j, i, vc.first, vc.second);
                std::this_thread::yield();
                iom_2->pushDecision(j, i, vc.first, vc.second);
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
        if(resultList_1[i]->groupId != resultList_2[i]->groupId || resultList_1[i]->blockId != resultList_2[i]->blockId) {
            if (!printFlag) {
                resultList_1[i-1]->printDebugString();
                resultList_2[i-1]->printDebugString();
            }
            printFlag = true;
        }
        if (printFlag) {
            resultList_1[i]->printDebugString();
            CHECK(resultList_1[i-1]->mustLessThan(resultList_1[i]));
            resultList_2[i]->printDebugString();
            CHECK(resultList_2[i-1]->mustLessThan(resultList_2[i]));
            count++;
        }
        if (count == 100) {
            CHECK(false);
        }
    }
    CHECK(!printFlag);
}
