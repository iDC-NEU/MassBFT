//
// Created by peng on 2/18/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "peer/replicator/mr_block_receiver.h"
#include "tests/block_fragment_generator_utils.h"
#include "tests/proto_block_utils.h"

#include "common/matrix_2d.h"

class MRBlockReceiverTest : public ::testing::Test {
protected:
    using ConfigPtr = peer::SingleRegionBlockReceiver::ConfigPtr;
    using Config = peer::SingleRegionBlockReceiver::Config;

    void SetUp() override {
    };

    void TearDown() override {
    };

    static std::vector<ConfigPtr> GenerateNodesConfig(int groupId, int count, int portOffset) {
        std::vector<ConfigPtr> nodesConfig;
        for (int i = 0; i < count; i++) {
            auto cfg = std::make_shared<Config>();
            auto nodeCfg = std::make_shared<util::NodeConfig>();
            nodeCfg->groupId = groupId;
            nodeCfg->nodeId = i;
            nodeCfg->ski = std::to_string(groupId) + "_" + std::to_string(i);
            cfg->nodeConfig = std::move(nodeCfg);
            cfg->addr() = "127.0.0.1";
            cfg->port = 51200 + portOffset + i;
            nodesConfig.push_back(std::move(cfg));
        }
        return nodesConfig;
    }

};

TEST_F(MRBlockReceiverTest, TestBlockSignValidate) {
    auto bfgUtils = std::make_unique<tests::BFGUtils>();

    // 3 regions, each region 4 nodes+1 sender
    bfgUtils->addCFG(4, 8, 1, 5);
    bfgUtils->addCFG(4, 8, 1, 5);
    bfgUtils->addCFG(4, 8, 1, 5);

    std::vector<ConfigPtr> configList;
    for (int i=0; i<3; i++) {
        auto ret = GenerateNodesConfig(i, 4, i*4);
        std::move(ret.begin(), ret.end(), std::back_inserter(configList));
    }

    // spin up servers
    util::Matrix2D<std::unique_ptr<util::ZMQInstance>> servers(3, 4);
    for (int i=0; i<3; i++) {
        for(int j=0; j<4; j++) {
            auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200 + i*4 +j);
            ASSERT_TRUE(sender != nullptr) << "Create instance failed";
            servers(i, j) = std::move(sender);
        }
    }

    // Init bfg
    std::shared_ptr<util::thread_pool_light> tpForBFGAndBCCSP(new util::thread_pool_light());
    auto bfg = std::make_shared<peer::BlockFragmentGenerator>(bfgUtils->cfgList, tpForBFGAndBCCSP.get());
    // Init bccsp
    auto bccsp = std::make_shared<util::BCCSP>(std::make_unique<util::DefaultKeyStorage>());
    // Init storage
    auto storage = std::make_shared<peer::MRBlockStorage>(3);   // 3 regions

    auto mr = peer::MRBlockReceiver::NewMRBlockReceiver(configList, bfg, bfgUtils->cfgList);
    ASSERT_TRUE(mr != nullptr);

    mr->setBCCSPWithThreadPool(bccsp, tpForBFGAndBCCSP);
    mr->setStorage(storage);

    // prepare keys
    for (int i=0; i<3; i++) {
        for(int j=0; j<4; j++) {
            bccsp->generateED25519Key(std::to_string(i) + "_" + std::to_string(j), false);
        }
    }
    // prepare a mock block
    std::string blockRaw;
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    block->metadata.consensusSignatures.clear();
    block->header.number = 0;
    auto pos = block->serializeToString(&blockRaw);
    ASSERT_TRUE(pos.valid) << "serialize block failed!";
    // sign the body and write back
    std::vector<std::string> regionBlockRaw(3);
    for (int i=0; i<3; i++) {
        for(int j=0; j<4; j++) {
            auto ski = std::to_string(i) + "_" + std::to_string(j);
            auto key = bccsp->GetKey(ski);
            std::string_view serHBody(blockRaw.data()+pos.headerPos, pos.execResultPos-pos.headerPos);
            ASSERT_TRUE(key->Private()) << "Can not sign header+body!";
            auto ret = key->Sign(serHBody.data(), serHBody.size());
            ASSERT_TRUE(ret) << "Sig validate failed, ski: " << ski;
            // push back the signature
            block->metadata.consensusSignatures.push_back({ski, key->PublicBytes(), *ret});
        }
        block->serializeToString(&regionBlockRaw[i]);
    }

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_TRUE(mr->checkAndStartService(0)) << "can not start mr";

    // encode to fragments, and send them
    for (int i=0; i<3; i++) {
        auto context = bfg->getEmptyContext(bfgUtils->cfgList[i]);
        context->initWithMessage(regionBlockRaw[i]);
        std::vector<std::string> serializedFragment(4);
        for (int j = 0; j < 4; j++) {
            proto::EncodeBlockFragment fragment{0, {}, {}, static_cast<uint32_t>(j * 1), static_cast<uint32_t>((j + 1) * 1), {}};
            fragment.size = regionBlockRaw[i].size();
            std::string msgBuf;
            CHECK(context->serializeFragments(fragment.start, fragment.end, msgBuf)) << "create fragment failed!";
            fragment.encodeMessage = msgBuf;    // string view
            fragment.root = context->getRoot();
            // serialize to string
            zpp::bits::out out(serializedFragment[j]);
            if(failure(out(fragment))) {
                CHECK(false) << "Encode message fragment failed!";
            }
        }
        // send fragment to corresponding peer
        for (int j = 0; j < 4; j++) {
            servers(i, j)->send(std::move(serializedFragment[j]));
        }
    }

    for (int i=0; i<3; i++) {
        storage->waitForNewBlock(i);
    }

    for (int i=0; i<3; i++) {
        auto maxId = storage->getMaxStoredBlockNumber(i);
        ASSERT_TRUE(maxId == 0) << "store block failed, id: " << maxId;
        auto recBlock = storage->getBlock(i, 0);
        ASSERT_TRUE(*recBlock->getSerializedMessage() == regionBlockRaw[i]);
    }

}