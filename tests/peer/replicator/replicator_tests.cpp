//
// Created by user on 23-3-20.
//

#include "peer/replicator/replicator.h"
#include "common/matrix_2d.h"
#include "common/timer.h"

#include "gtest/gtest.h"
#include "tests/proto_block_utils.h"

class ReplicatorTest : public ::testing::Test {
public:
    ReplicatorTest() {
        util::OpenSSLSHA256::initCrypto();
    }

protected:
    void SetUp() override {
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::Timer::sleep_sec(1);
        util::MetaRpcServer::Stop();
    };

    static std::unordered_map<int, std::vector<util::NodeConfigPtr>> GenerateNodesMap() {
        auto ret = std::unordered_map<int, std::vector<util::NodeConfigPtr>>();
        ret[0] = tests::ProtoBlockUtils::GenerateNodesConfig(0, 4);
        ret[1] = tests::ProtoBlockUtils::GenerateNodesConfig(1, 5);
        ret[2] = tests::ProtoBlockUtils::GenerateNodesConfig(2, 11);
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

    static void StartTest(const std::function<void(
            const std::unordered_map<int, std::vector<util::NodeConfigPtr>>& nodes,
            std::shared_ptr<util::BCCSP>& bccsp,
            util::Matrix2D<std::shared_ptr<peer::MRBlockStorage>>& storage,
            util::Matrix2D<std::unique_ptr<peer::Replicator>>& replicators)>& callback) {
        auto bccsp = CreateBCCSP();
        auto nodes = GenerateNodesMap();
        // ---- init port map
        std::unordered_map<int, int> regionNodesCount;
        for (const auto& it: nodes) {
            regionNodesCount[it.first] = (int)it.second.size();
        }
        auto zmqPortUtilMap = util::ZMQPortUtil::InitPortsConfig(51200, regionNodesCount, false);
        // ----
        auto startAt = GenerateStartAt();
        util::Matrix2D<std::unique_ptr<peer::Replicator>> matrix(3, 31);
        util::Matrix2D<std::shared_ptr<peer::MRBlockStorage>> storage(3, 31);
        for (int i=0; i<(int)nodes.size(); i++) {
            for (int j =0; j<(int)nodes[i].size(); j++) {
                auto replicator = std::make_unique<peer::Replicator>(nodes, nodes[i][j]);
                replicator->setBCCSPWithThreadPool(bccsp, std::make_shared<util::thread_pool_light>());
                replicator->setPortUtilMap(zmqPortUtilMap);
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
        callback(nodes, bccsp, storage, matrix);
    }

    static void StartCrashTest(bool senderCrash, bool receiverCrash) {
        StartTest([&](const auto& nodes, auto& bccsp, auto& storage, auto& matrix) ->void {
            CHECK(nodes.at(2).size() == 11);
            if (senderCrash) {
                // tear down a sender node
                matrix(0, 0).reset();
            }
            if (receiverCrash) {
                // tear down 3 receiver nodes
                matrix(2, 0).reset();
                matrix(2, 1).reset();
                matrix(2, 2).reset();
            }
            // region 0 send block to other regions
            auto blockForRo = CreateMockBlock(0, 0, 4, bccsp);
            for (int j = (senderCrash ? 1 : 0); j < (int)nodes.at(0).size(); j++) {
                storage(0, j)->insertBlock(0, blockForRo);
                storage(0, j)->onReceivedNewBlock(0, blockForRo->header.number);
            }
            std::string blockForRoRaw;
            blockForRo->serializeToString(&blockForRoRaw);
            // region 2 can still generate message
            for (int j = (receiverCrash ? 3 : 0); j < (int) nodes.at(1).size(); j++) {
                storage(2, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
                auto ret = storage(2, j)->getBlock(0, blockForRo->header.number);
                ASSERT_TRUE(ret != nullptr);
                std::string retRaw;
                ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
                ASSERT_TRUE(retRaw == blockForRoRaw);
            }
            // region 1 must be alright
            for (int j = 0; j < (int) nodes.at(1).size(); j++) {
                storage(1, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
                auto ret = storage(1, j)->getBlock(0, blockForRo->header.number);
                ASSERT_TRUE(ret != nullptr);
                std::string retRaw;
                ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
                ASSERT_TRUE(retRaw == blockForRoRaw);
            }
        });
    }
};

TEST_F(ReplicatorTest, TestInitialize) {
    auto bccsp = CreateBCCSP();
    auto nodes = GenerateNodesMap();
    // ---- init port map
    std::unordered_map<int, int> regionNodesCount;
    for (const auto& it: nodes) {
        regionNodesCount[it.first] = (int)it.second.size();
    }
    auto zmqPortUtilMap = util::ZMQPortUtil::InitPortsConfig(51200, regionNodesCount, false);
    // ----
    auto replicator = std::make_unique<peer::Replicator>(nodes, nodes[0][0]);
    replicator->setBCCSPWithThreadPool(bccsp, std::make_shared<util::thread_pool_light>());
    replicator->setPortUtilMap(zmqPortUtilMap);
    ASSERT_TRUE(replicator->initialize());
    auto startAt = GenerateStartAt();
    ASSERT_TRUE(replicator->startReceiver(startAt));
    ASSERT_TRUE(replicator->startSender(startAt[0]));
}

TEST_F(ReplicatorTest, TestSendNoError) {
    StartCrashTest(false, false);
}

TEST_F(ReplicatorTest, TestSenderCrash) {
    StartCrashTest(true, false);
}

TEST_F(ReplicatorTest, TestReceiverCrash) {
    StartCrashTest(false, true);
}

TEST_F(ReplicatorTest, TestSenderAndReceiverCrash) {
    StartCrashTest(true, true);
}

TEST_F(ReplicatorTest, TestSenderByzantineWithReceiverCrash) {
    StartTest([](const auto& nodes, auto& bccsp, auto& storage, auto& matrix) ->void {
        CHECK(nodes.at(2).size() == 11);
        // tear down 3 receiver nodes
        matrix(2, 10).reset();
        matrix(2, 9).reset();
        matrix(2, 8).reset();

        // Byzantine sender, no signature
        auto fakeBlock = CreateMockBlock(0, 0, 0, bccsp);
        fakeBlock->header.previousHash = {"fakeHash"};
        storage(0, (int)nodes.at(0).size()-1)->insertBlock(0, fakeBlock);
        storage(0, (int)nodes.at(0).size()-1)->onReceivedNewBlock(0, fakeBlock->header.number);

        // Honest nodes in region 0 send block to other regions
        auto blockForRo = CreateMockBlock(0, 0, 4, bccsp);
        for (int j =0; j<(int)nodes.at(0).size()-1; j++) {
            storage(0, j)->insertBlock(0, blockForRo);
            storage(0, j)->onReceivedNewBlock(0, blockForRo->header.number);
        }

        std::string blockForRoRaw;
        blockForRo->serializeToString(&blockForRoRaw);
        // region 2 can still generate message
        for (int j = 0; j < 8; j++) {
            storage(2, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
            auto ret = storage(2, j)->getBlock(0, blockForRo->header.number);
            ASSERT_TRUE(ret != nullptr);
            std::string retRaw;
            ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
            ASSERT_TRUE(retRaw == blockForRoRaw);
        }
        // region 1 must be alright
        for (int j = 0; j < (int) nodes.at(1).size(); j++) {
            storage(1, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
            auto ret = storage(1, j)->getBlock(0, blockForRo->header.number);
            ASSERT_TRUE(ret != nullptr);
            std::string retRaw;
            ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
            ASSERT_TRUE(retRaw == blockForRoRaw);
        }
    });
}

TEST_F(ReplicatorTest, TestSenderByzantineWithReceiverCrashMultipleBlocks) {
    StartTest([](const auto& nodes, auto& bccsp, auto& storage, auto& matrix) ->void {
        CHECK(nodes.at(2).size() == 11);
        // tear down 3 receiver nodes
        matrix(2, 10).reset();
        matrix(2, 9).reset();
        matrix(2, 8).reset();

        std::vector<std::shared_ptr<proto::Block>> byzantineBlocks;
        std::vector<std::shared_ptr<proto::Block>> honestBlocks;

        for (int blkNumber=0; blkNumber<100; blkNumber++) {
            LOG(INFO) << "Sending block " << blkNumber;
            // Byzantine sender, no signature
            auto fakeBlock = CreateMockBlock(0, 0, 0, bccsp);
            fakeBlock->header.previousHash = {"fakeHash"};
            storage(0, (int)nodes.at(0).size()-1)->insertBlock(0, fakeBlock);
            storage(0, (int)nodes.at(0).size()-1)->onReceivedNewBlock(0, fakeBlock->header.number);
            byzantineBlocks.push_back(std::move(fakeBlock));

            // Honest nodes in region 0 send block to other regions
            auto blockForRo = CreateMockBlock(0, 0, 4, bccsp);
            for (int j =0; j<(int)nodes.at(0).size()-1; j++) {
                storage(0, j)->insertBlock(0, blockForRo);
                storage(0, j)->onReceivedNewBlock(0, blockForRo->header.number);
            }
            honestBlocks.push_back(std::move(blockForRo));
        }

        for (int blkNumber=0; blkNumber<100; blkNumber++) {
            LOG(INFO) << "Validate block " << blkNumber;
            std::string blockForRoRaw;
            auto& blockForRo = honestBlocks[blkNumber];
            blockForRo->serializeToString(&blockForRoRaw);
            // region 2 can still generate message
            for (int j = 0; j < 8; j++) {
                storage(2, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
                auto ret = storage(2, j)->getBlock(0, blockForRo->header.number);
                ASSERT_TRUE(ret != nullptr);
                std::string retRaw;
                ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
                ASSERT_TRUE(retRaw == blockForRoRaw);
            }
            // region 1 must be alright
            for (int j = 0; j < (int) nodes.at(1).size(); j++) {
                storage(1, j)->waitForNewBlock(0, (int)blockForRo->header.number, nullptr);
                auto ret = storage(1, j)->getBlock(0, blockForRo->header.number);
                ASSERT_TRUE(ret != nullptr);
                std::string retRaw;
                ASSERT_TRUE(ret->serializeToString(&retRaw).valid);
                ASSERT_TRUE(retRaw == blockForRoRaw);
            }
        }
    });
}
