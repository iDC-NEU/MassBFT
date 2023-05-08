//
// Created by user on 23-5-8.
//

#include "peer/consensus/block_order/global_ordering.h"
#include "common/timer.h"
#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

using namespace peer::consensus::v2;

class GlobalBlockOrderingTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static auto GetRaftNodes() {
        auto regions = tests::ProtoBlockUtils::GenerateNodesConfig(0, 2, 0);
        auto region1 = tests::ProtoBlockUtils::GenerateNodesConfig(1, 2, 10);
        regions.insert(regions.end(), region1.begin(), region1.end());
        return regions;
    }

    static auto GetLocalNodes(int groupId) {
        return tests::ProtoBlockUtils::GenerateNodesConfig(groupId, 4, groupId*10+4);
    }
};

TEST_F(GlobalBlockOrderingTest, Region0Send) {
    auto raftNodes = GetRaftNodes();
    std::vector<int> raftLeaders {0, 2};    // 2 regions
    auto localNodes0 = GetLocalNodes(0);
    auto localNodes1 = GetLocalNodes(1);
    // prepare callback
    std::vector<std::vector<std::pair<int, int>>> retValue(8);
    bthread::CountdownEvent ce(8);  // 8 nodes total
    std::vector<bool> sent(8);

    auto callback = [&] (int i, int regionId, int blockId) ->bool {
        if (retValue[i].size() >= 15000) {
            if (!sent[i]) {
                sent[i] = true;
                ce.signal();
            }
        } else {
            retValue[i].emplace_back(regionId, blockId);
        }
        return true;
    };

    // spin up nodes in region 0
    std::vector<std::unique_ptr<BlockOrder>> regions(8);
    for (auto i: {0, 1, 2, 3}) {
        auto& me = localNodes0[i]->nodeConfig;
        auto orderCAB = std::make_unique<OrderACB>([&callback, i=i](int regionId, int blockId) ->bool {
                return callback(i, regionId, blockId);
        });
        regions[i] = BlockOrder::NewBlockOrder(localNodes0, raftNodes, raftLeaders, me, std::move(orderCAB));
        ASSERT_TRUE(regions[i] != nullptr);
    }
    // spin up nodes in region 1
    for (auto i: {4, 5, 6, 7}) {
        auto& me = localNodes1[i-4]->nodeConfig;
        auto orderCAB = std::make_unique<OrderACB>([&callback, i=i](int regionId, int blockId) ->bool {
            return callback(i, regionId, blockId);
        });
        regions[i] = BlockOrder::NewBlockOrder(localNodes1, raftNodes, raftLeaders, me, std::move(orderCAB));
        ASSERT_TRUE(regions[i] != nullptr);
    }
    // leaders send proposals
    CHECK(regions[0]->isLeader() && regions[4]->isLeader());
    // wait until ready
    ASSERT_TRUE(regions[0]->waitUntilRaftReady());
    ASSERT_TRUE(regions[4]->waitUntilRaftReady());

    auto voteFunc = [&](int myIdx, int targetGroup) {
        for (int i=0; i< 10000; i++) {
            auto ret = regions[myIdx]->voteNewBlock(targetGroup, i);
            CHECK(ret);
        }
    };

    // group 0, node id = 0
    std::thread sender_1(voteFunc, 0, 0);
    std::thread sender_2(voteFunc, 0, 1);
    // group 1, node id = 4
    std::thread sender_4(voteFunc, 4, 0);
    std::thread sender_5(voteFunc, 4, 1);
    LOG(INFO) << "start sending message";
    sender_1.join();
    sender_2.join();

    sender_4.join();
    sender_5.join();
    LOG(INFO) << "finish sending message";

    // need to check the result
    ce.wait();
    for (int i=1; i<(int)retValue.size(); i++) {
        ASSERT_TRUE(retValue[0] == retValue[i]);
    }
    LOG(INFO) << "Test successfully finished, size: " << retValue[0].size();
    LOG(INFO) << "List size: " << retValue.size();
    LOG(INFO) << "Last value: " << retValue[0].back().first << ", " << retValue[0].back().second;
    util::Timer::sleep_sec(10);
}