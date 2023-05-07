//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/async_agreement.h"
#include "common/timer.h"
#include "tests/proto_block_utils.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

using namespace peer::consensus;

class MockACB : public AsyncAgreementCallback {
public:
    MockACB(int groupCount, std::shared_ptr<util::ZMQInstanceConfig> node)
            : AsyncAgreementCallback(groupCount), localNode(std::move(node)) {}

    // called by orderManager in base class
    bool onDeliver(int chainId, int blockNumber) override {
        LOG(INFO) << "{ " << localNode->nodeConfig->groupId << ", " << localNode->nodeConfig->nodeId << " }: "
                  << chainId << ", " << blockNumber;
        return true;
    }

    // called by localDistributor
    bool onBroadcast(std::string decision) override {
        return applyRawBlockOrder(decision);
    }

private:
    std::shared_ptr<util::ZMQInstanceConfig> localNode;
};


class AsyncAgreementTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static auto InitNodes() {
        auto regions = tests::ProtoBlockUtils::GenerateNodesConfig(0, 2, 0);
        auto region1 = tests::ProtoBlockUtils::GenerateNodesConfig(1, 2, 10);
        auto region2 = tests::ProtoBlockUtils::GenerateNodesConfig(2, 2, 20);
        regions.insert(regions.end(), region1.begin(), region1.end());
        regions.insert(regions.end(), region2.begin(), region2.end());
        return regions;
    }
};

TEST_F(AsyncAgreementTest, TestFSM) {
    auto nodes = InitNodes();

    // Test with only one master
    std::vector<braft::PeerId> subPeers;
    for (auto& it: nodes) {
        braft::PeerId tmp;
        CHECK(AsyncAgreement::PeerIdFromConfig(it->addr(), it->port, 0, tmp));
        subPeers.push_back(tmp);
    }

    bthread::CountdownEvent ce((int)subPeers.size());

    auto multiRaft = std::make_shared<util::raft::MultiRaftFSM>("test_cluster");
    for (int i=0; i<(int)subPeers.size(); i++) {
        auto* fsm = new AgreementRaftFSM(subPeers[i], subPeers[0], multiRaft);
        auto& port = subPeers[i].addr.port;
        CHECK(util::DefaultRpcServer::AddRaftService(port) == 0);
        CHECK(util::DefaultRpcServer::Start(port) == 0);
        fsm->setOnApplyCallback([&](const butil::IOBuf&)->bool {
            ce.signal();
            return true;
        });
        CHECK(multiRaft->start(subPeers, i, fsm) == 0);
    }

    auto* leader = multiRaft->find_node(subPeers[0]);
    CHECK(leader);
    auto* fsm = dynamic_cast<AgreementRaftFSM*>(multiRaft->find_fsm(subPeers[0]));
    CHECK(fsm && fsm->ready());
    // construct data to be sent
    butil::IOBuf data;
    data.append("content");
    braft::Task task;
    task.data = &data;
    leader->apply(task);
    // ensure all follower received the data
    ce.wait();

    for (auto & subPeer : subPeers) {
        auto& port = subPeer.addr.port;
        util::DefaultRpcServer::Stop(port);
    }
}

TEST_F(AsyncAgreementTest, TestAgreement) {
    auto nodes = InitNodes();
    // each elem in aaList is a node instance(a node both as follower in n-1 and leader in 1 instance)
    std::vector<std::unique_ptr<AsyncAgreement>> aaList;

    for (const auto& it : nodes) {
        auto acb = std::make_unique<MockACB>(3, it);
        auto aa = AsyncAgreement::NewAsyncAgreement(it, std::move(acb));
        if (aa == nullptr) {
            CHECK(false) << "init failed";
        }
        aaList.push_back(std::move(aa));
    }
    // start all peers
    for (int i=0; i<(int)nodes.size(); i++) {
        // each peer contains 3 multi-raft instance, 0, 2, 4 are leaders, j/2 are group id
        for (auto j: {0, 2, 4}) {
            CHECK(aaList[i]->startCluster(nodes, j, j/2));
        }
    }
    // ensure leader
    for (auto i: {0, 2, 4}) {
        CHECK(aaList[i]->ready());
    }

    auto voteFunc = [&](int myIdx, int targetGroup) {
        for (int i=0; i< 10000; i++) {
            auto ret = aaList[myIdx]->onLeaderVotingNewBlock(targetGroup, i);
            CHECK(ret);
            util::Timer::sleep_ms(1);
        }
    };

    // group 0, node id = 0
    std::thread sender_1(voteFunc, 0, 0);
    std::thread sender_2(voteFunc, 0, 1);
    std::thread sender_3(voteFunc, 0, 2);
    // group 1, node id = 2
    std::thread sender_4(voteFunc, 2, 0);
    std::thread sender_5(voteFunc, 2, 1);
    std::thread sender_6(voteFunc, 2, 2);
    // group 2, node id = 4
    std::thread sender_7(voteFunc, 4, 0);
    std::thread sender_8(voteFunc, 4, 1);
    std::thread sender_9(voteFunc, 4, 2);

    sender_1.join();
    sender_2.join();
    sender_3.join();
    sender_4.join();
    sender_5.join();
    sender_6.join();
    sender_7.join();
    sender_8.join();
    sender_9.join();
}