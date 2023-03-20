//
// Created by user on 23-3-20.
//

#include "peer/replicator/replicator.h"
#include "common/matrix_2d.h"

#include "gtest/gtest.h"
#include "tests/proto_block_utils.h"

class ReplicatorTest : public ::testing::Test {
public:
    ReplicatorTest() {
        util::OpenSSLSHA256::initCrypto();
        util::ReliableZmqServer::AddRPCService();
    }

protected:
    void SetUp() override {
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
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

    static std::shared_ptr<proto::Block> CreateMockBlock(int blockNumber, int regionId, int maxNodeId, std::shared_ptr<util::BCCSP>& bccsp) {
        std::string blockRaw;
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->metadata.consensusSignatures.clear();
        block->header.number = blockNumber;
        auto pos = block->serializeToString(&blockRaw);
        for(int j=0; j<maxNodeId; j++) {
            auto ski = std::to_string(regionId) + "_" + std::to_string(j);
            auto key = bccsp->GetKey(ski);
            std::string_view serHBody(blockRaw.data()+pos.headerPos, pos.execResultPos-pos.headerPos);
            auto ret = key->Sign(serHBody.data(), serHBody.size());
            CHECK(ret) << "Sig validate failed, ski: " << ski;
            block->metadata.consensusSignatures.push_back({ski, key->PublicBytes(), *ret});
        }
        return block;
    }

    static void StartTest(bool crash = false) {
        auto bccsp = CreateBCCSP();
        auto nodes = GenerateNodesMap();
        auto startAt = GenerateStartAt();
        util::Matrix2D<std::unique_ptr<peer::Replicator>> matrix(3, 6);
        util::Matrix2D<std::shared_ptr<peer::MRBlockStorage>> storage(3, 6);
        for (int i=0; i<(int)nodes.size(); i++) {
            for (int j =0; j<(int)nodes[i].size(); j++) {
                auto replicator = std::make_unique<peer::Replicator>(nodes, nodes[i][j]);
                replicator->setBCCSP(bccsp);
                ASSERT_TRUE(replicator->initialize());
                ASSERT_TRUE(replicator->startReceiver(startAt));
                storage(i, j) = replicator->getStorage();
                matrix(i, j) = std::move(replicator);
            }
        }
        for (int i=0; i<(int)nodes.size(); i++) {
            for (int j = 0; j < (int) nodes[i].size(); j++) {
                ASSERT_TRUE(matrix(i, j)->startSender(startAt[0]));
            }
        }
        // -------------INIT COMPLETE-------------
        // region 0 send block to other regions
        // no error version
        auto blockForRo = CreateMockBlock(0, 0, 4, bccsp);
        for (int j =0; j<(int)nodes[0].size(); j++) {
            storage(0, j)->insertBlock(0, blockForRo);
            storage(0, j)->onReceivedNewBlock(0, blockForRo->header.number);
        }

        std::string blockForRoRaw;
        blockForRo->serializeToString(&blockForRoRaw);
        for (int i = 1; i < (int)nodes.size(); i++) {
            for (int j = 0; j < (int) nodes[i].size(); j++) {
                storage(i, j)->waitForNewBlock((int)blockForRo->header.number, nullptr);
                auto ret = storage(i, j)->getBlock(0, blockForRo->header.number);
                std::string retRaw;
                ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
                ASSERT_TRUE(retRaw == blockForRoRaw);
            }
        }
    }
};


TEST_F(ReplicatorTest, TestInitialize) {
    auto bccsp = CreateBCCSP();
    auto nodes = GenerateNodesMap();
    auto replicator = std::make_unique<peer::Replicator>(nodes, nodes[0][0]);
    replicator->setBCCSP(bccsp);
    ASSERT_TRUE(replicator->initialize());
    auto startAt = GenerateStartAt();
    ASSERT_TRUE(replicator->startReceiver(startAt));
    ASSERT_TRUE(replicator->startSender(startAt[0]));
}

TEST_F(ReplicatorTest, TestSendNoError) {
    StartTest(false);
}
