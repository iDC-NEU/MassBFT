//
// Created by user on 23-3-10.
//

#include "peer/replicator/v2/block_sender.h"

#include "tests/block_fragment_generator_utils.h"
#include "tests/proto_block_utils.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

class BlockSenderTestV2 : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA256::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

    static std::string prepareBlock(proto::BlockNumber blockNumber) {
        // prepare a mock block
        std::string blockRaw;
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->metadata.consensusSignatures.clear();
        block->header.number = blockNumber;
        block->header.dataHash = {"dataHash"};
        block->header.previousHash = {"previousHash"};
        auto pos = block->serializeToString(&blockRaw);
        CHECK(pos.valid) << "serialize block failed!";
        return blockRaw;
    }

};

TEST_F(BlockSenderTestV2, IntrgrateTest4_4) {
    // Init storage
    std::shared_ptr<peer::MRBlockStorage> storageList = std::make_shared<peer::MRBlockStorage>(3);
    // spin up servers
    std::vector<peer::v2::MRBlockSender::ConfigPtr> configList;
    // generate sender list (4 servers)
    std::vector<std::unique_ptr<peer::v2::MRBlockSender>> servers(4);
    for (int i = 0; i < 3; i++) {
        auto regionBfgConf = tests::ProtoBlockUtils::GenerateNodesConfig(i, 4, i * 4);
        std::move(regionBfgConf.begin(), regionBfgConf.end(), std::back_inserter(configList));
    }
    // init receivers
    // receivers[0] leaves nullptr;
    util::Matrix2D<std::shared_ptr<util::ReliableZmqServer>> receivers(3, 4);
    for (int i = 1; i < 3; i++) {
        for (int j = 0; j < 4; j++) {
            util::ReliableZmqServer::NewSubscribeServer(configList[i * 4 + j]->port);
            receivers(i, j) = util::ReliableZmqServer::GetSubscribeServer(configList[i * 4 + j]->port);
        }
    }
    // change config list to map
    std::unordered_map<int, std::vector<peer::v2::MRBlockSender::ConfigPtr>> configMap;
    for (const auto& it:configList) {
        configMap[it->nodeConfig->groupId].push_back(it);
    }
    std::unordered_map<int, int> regionNodesCount;
    for (const auto& it:configMap) {
        regionNodesCount[it.first] = (int)it.second.size();
    }
    // we use ret.first to init bfg
    auto bfgWp = std::make_shared<util::thread_pool_light>();
    // init bfg
    auto ret = peer::v2::FragmentUtil::GenerateAllConfig(regionNodesCount, 0, 0);

    std::vector<peer::BlockFragmentGenerator::Config> bfgConfigList;
    bfgConfigList.push_back({.dataShardCnt=2, .parityShardCnt=2, .instanceCount = 1, .concurrency = 1});
    for (auto& it: ret.first) {
        bfgConfigList.push_back(it.second);
    }
    auto bfg = std::make_shared<peer::BlockFragmentGenerator>(bfgConfigList, bfgWp.get());

    std::unordered_map<int, peer::BlockFragmentGenerator> bfgMap;
    // init senders, local_region==0
    auto bsWp = std::make_shared<util::thread_pool_light>();
    for (int i = 0; i < 4; i++) {
        // ret.first is redundant
        ret = peer::v2::FragmentUtil::GenerateAllConfig(regionNodesCount, 0, i);
        auto sender = peer::v2::MRBlockSender::NewMRBlockSender(configMap, ret.second, 0, bsWp);
        ASSERT_TRUE(sender != nullptr) << "start sender failed";sender->setStorage(storageList);
        sender->setBFGWithConfig(bfg, ret.first);
        ASSERT_TRUE(sender->checkAndStart(0)) << "start sender failed";
        servers[i] = std::move(sender);
    }

    for (int bkNum = 0; bkNum < 100; bkNum++) {
        // send the data
        auto regionBlockRaw = prepareBlock(bkNum);
        std::unique_ptr<proto::Block> regionBlock(new proto::Block);
        regionBlock->deserializeFromString(std::string(regionBlockRaw));
        // Region r broadcasts block to all other regions
        storageList->insertBlock(0, std::move(regionBlock));
        storageList->onReceivedNewBlock(0, bkNum);
        // receive the data
        for (int i = 1; i < 3; i++) {
            auto message = receivers(i, 1)->waitReady();
            ASSERT_TRUE(message != std::nullopt);
            auto message2 = receivers(i, 3)->waitReady();
            ASSERT_TRUE(message2 != std::nullopt);
            std::string_view sv1(reinterpret_cast<const char*>(message->data()), message->size());
            std::string_view sv2(reinterpret_cast<const char*>(message2->data()), message2->size());
            proto::EncodeBlockFragment ebf1;
            ebf1.deserializeFromString(sv1);
            proto::EncodeBlockFragment ebf2;
            ebf2.deserializeFromString(sv2);
            auto context = bfg->getEmptyContext(bfgConfigList[0]);
            ASSERT_TRUE(context->validateAndDeserializeFragments(ebf1.root, ebf1.encodeMessage, (int)ebf1.start, (int)ebf1.end));
            ASSERT_TRUE(context->validateAndDeserializeFragments(ebf2.root, ebf2.encodeMessage, (int)ebf2.start, (int)ebf2.end));
            std::string blockRaw;
            ASSERT_TRUE(context->regenerateMessage((int)ebf1.size, blockRaw));
            ASSERT_TRUE(regionBlockRaw == blockRaw);
        }
    }
}