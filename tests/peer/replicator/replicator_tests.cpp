//
// Created by user on 23-3-20.
//

#include "peer/replicator/replicator.h"

#include "gtest/gtest.h"

class ReplicatorTest : public ::testing::Test {
public:
    ReplicatorTest() {
        util::OpenSSLSHA256::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    }

protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static std::unordered_map<int, std::vector<util::NodeConfigPtr>> GenerateNodesMap() {
        auto ret = std::unordered_map<int, std::vector<util::NodeConfigPtr>>();
        auto createNode = [](int nodeId, int groupId) {
            auto node = std::make_shared<util::NodeConfig>();
            node->nodeId = nodeId;
            node->groupId = groupId;
            node->ip = "127.0.0.1";
            node->ski = std::to_string(nodeId) + "_" + std::to_string(groupId);
            return node;
        };
        for(int i=0; i<4; i++) {
            ret[0].push_back(createNode(i, 0));
        }
        for(int i=0; i<5; i++) {
            ret[1].push_back(createNode(i, 1));
        }
        for(int i=0; i<6; i++) {
            ret[2].push_back(createNode(i, 2));
        }
        return ret;
    }

    static std::unordered_map<int, proto::BlockNumber> GenerateStartAt() {
        auto ret = std::unordered_map<int, proto::BlockNumber>();
        for(int i=0; i<3; i++) {
            ret[i] = 0;
        }
        return ret;
    }
    static auto CreateBCCSP() {
        auto bccsp = std::make_shared<util::BCCSP>(std::make_unique<util::DefaultKeyStorage>());
        for (int i=0; i<10; i++) {
            for(int j=0; j<10; j++) {
                bccsp->generateED25519Key(std::to_string(i) + "_" + std::to_string(j), false);
            }
        }
        return bccsp;
    }
};


TEST_F(ReplicatorTest, TestInitialize) {
    // Init bccsp
    auto bccsp = CreateBCCSP();
    auto nodes = GenerateNodesMap();
    auto replicator = std::make_unique<peer::Replicator>(nodes, nodes[0][0]);
    replicator->setBCCSP(bccsp);
    ASSERT_TRUE(replicator->initialize());
    auto startAt = GenerateStartAt();
    ASSERT_TRUE(replicator->startReceiver(startAt));
    ASSERT_TRUE(replicator->startSender(startAt[0]));
}
