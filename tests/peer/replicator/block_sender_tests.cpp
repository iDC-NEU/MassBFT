//
// Created by peng on 2/19/23.
//

#include "peer/replicator/block_sender.h"
#include "peer/replicator/mr_block_receiver.h"
#include "tests/block_fragment_generator_utils.h"
#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class BlockSenderTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    // 3 regions, 4 nodes version
    std::vector<std::string> prepareBlock(proto::BlockNumber blockNumber, util::BCCSP* bccsp) {
        // prepare a mock block
        std::string blockRaw;
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->metadata.consensusSignatures.clear();
        block->header.number = blockNumber;
        auto pos = block->serializeToString(&blockRaw);
        CHECK(pos.valid) << "serialize block failed!";
        // sign the body and write back
        std::vector<std::string> regionBlockRaw(3);
        for (int i=0; i<3; i++) {
            for(int j=0; j<4; j++) {
                auto ski = std::to_string(i) + "_" + std::to_string(j);
                auto key = bccsp->GetKey(ski);
                std::string_view serHBody(blockRaw.data()+pos.headerPos, pos.execResultPos-pos.headerPos);
                CHECK(key->Private()) << "Can not sign header+body!";
                auto ret = key->Sign(serHBody.data(), serHBody.size());
                CHECK(ret) << "Sig validate failed, ski: " << ski;
                // push back the signature
                block->metadata.consensusSignatures.push_back({ski, key->PublicBytes(), *ret});
            }
            block->serializeToString(&regionBlockRaw[i]);
        }
        return regionBlockRaw;
    }
};

TEST_F(BlockSenderTest, IntrgrateTest) {
    auto bfgUtils = std::make_unique<tests::BFGUtils>();

    // 3 regions, each region 4 nodes+1 sender
    bfgUtils->addCFG(4, 8, 1, 5);
    bfgUtils->addCFG(4, 8, 1, 5);
    bfgUtils->addCFG(4, 8, 1, 5);

    // Init bfg
    std::shared_ptr<util::thread_pool_light> tpForBFGAndBCCSP(new util::thread_pool_light());
    auto bfg = std::make_shared<peer::BlockFragmentGenerator>(bfgUtils->cfgList, tpForBFGAndBCCSP.get());
    // Init storage
    std::vector<std::shared_ptr<peer::MRBlockStorage>> storageList(3);
    for (auto& it: storageList) {
        it = std::make_shared<peer::MRBlockStorage>(3);   // 3 regions
    }

    // spin up servers
    std::vector<tests::ProtoBlockUtils::ConfigPtr> configList;
    util::Matrix2D<std::unique_ptr<peer::BlockSender>> servers(3, 4);
    for (int i=0; i<3; i++) {
        auto regionBfgConf = tests::ProtoBlockUtils::GenerateNodesConfig(i, 4, i*4);
        for(int j=0; j<4; j++) {
            auto sender = std::make_unique<peer::BlockSender>();
            sender->setStorage(storageList[i]);
            sender->setBFG(bfgUtils->cfgList[i], bfg);
            sender->setLocalServerConfig(regionBfgConf[j], 4);
            auto ret = sender->checkAndStart(0);
            ASSERT_TRUE(ret) << "start sender failed";
            servers(i, j) = std::move(sender);
        }
        std::move(regionBfgConf.begin(), regionBfgConf.end(), std::back_inserter(configList));
    }

    auto mr = peer::MRBlockReceiver::NewMRBlockReceiver(0, configList, bfg, bfgUtils->cfgList);
    ASSERT_TRUE(mr != nullptr);

    // Init bccsp
    auto bccsp = std::make_shared<util::BCCSP>(std::make_unique<util::DefaultKeyStorage>());
    mr->setBCCSPWithThreadPool(bccsp, tpForBFGAndBCCSP);
    mr->setStorage(storageList[0]);

    // prepare keys
    for (int i=0; i<3; i++) {
        for(int j=0; j<4; j++) {
            bccsp->generateED25519Key(std::to_string(i) + "_" + std::to_string(j), false);
        }
    }

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_TRUE(mr->checkAndStartService(0)) << "can not start mr";

    for (int bkNum = 0; bkNum<100; bkNum++) {
        auto regionBlockRaw = prepareBlock(bkNum, bccsp.get());
        for (int i=0; i<3; i++) {
            std::unique_ptr<proto::Block> regionBlock(new proto::Block);
            regionBlock->deserializeFromString(std::string(regionBlockRaw[i]));
            // Region r broadcasts block to all other regions
            storageList[i]->insertBlock(i, std::move(regionBlock));
            storageList[i]->onReceivedNewBlock(i, bkNum);
        }
        // check the block data
        // Since we only deploy one receiving instance (region 0),
        // it will only receive block 0 from region 1 and region 2,
        // so it will only receive two blocks in total
        for (int i = 0; i < 3; i++) {
            int localRegionId = 0;
            while(!storageList[localRegionId]->waitForNewBlock(i, bkNum, nullptr));
            std::string buf;
            storageList[localRegionId]->getBlock(i, bkNum)->serializeToString(&buf);
            ASSERT_TRUE(regionBlockRaw[i] == buf) << "block mismatch!";
        }
    }
}