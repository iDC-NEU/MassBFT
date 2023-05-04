//
// Created by user on 23-4-5.
//

#include "peer/consensus/raft/v2/interchain_order_manager.h"
#include "common/timer.h"
#include "common/thread_pool_light.h"
#include <queue>

#include "bthread/countdown_event.h"
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

class OrderIterator {
public:
    OrderIterator(int total, peer::consensus::v2::InterChainOrderManager* iom) {
        _iom = iom;
        for (int i=0; i<total; i++) {
            _view.emplace_back(-1);
        }
    }

    OrderIterator(const OrderIterator&) = delete;

    std::unordered_map<int, int> getBlockOrder(int chainId, int blockId) {
        std::unique_lock guard(mutex);
        std::unordered_map<int, int> ret;
        for (int i=0; i<(int)_view.size(); i++) {
            ret[i] = _view[i];
        }
        auto it = _view[chainId];
        CHECK(it == blockId - 1);
        _view[chainId] = blockId;
        return ret;
    }

private:
    std::mutex mutex;
    peer::consensus::v2::InterChainOrderManager* _iom;
    std::vector<int> _view;
};


TEST_F(OrderManagerTest, TestDeterminsticOrder2) {
    std::vector<std::unique_ptr<OrderIterator>> oiList;
    oiList.reserve(3);
    for (int i=0; i<3; i++) {
        oiList.push_back(std::make_unique<OrderIterator>(3, iom.get()));
    }
    util::thread_pool_light tp;
    const peer::consensus::v2::InterChainOrderManager::Cell* lastCell = nullptr;
    iom->setDeliverCallback([&](const peer::consensus::v2::InterChainOrderManager::Cell* cell) {
        LOG(INFO) << "RESULT"; cell->printDebugString();
        if (lastCell != nullptr) {
            if(!lastCell->operator<(cell)) {
                CHECK(false);
            }
        }
        lastCell = cell;
    });

    auto func2 = [&](int id) {
        for (int i=0; i< 1000; i++) {
            for (int j=0; j<3; j++) {
                auto vc = oiList[j]->getBlockOrder(id, i);
                iom->pushDecision(id, i, std::move(vc));
                util::Timer::sleep_ms(15 - rand()%5 - id*2);
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
