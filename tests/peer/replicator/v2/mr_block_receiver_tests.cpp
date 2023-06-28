//
// Created by peng on 2/18/23.
//

#include "peer/replicator/v2/mr_block_receiver.h"
#include "peer/replicator/v2/fragment_util.h"
#include "tests/block_fragment_generator_utils.h"
#include "tests/proto_block_utils.h"
#include "common/matrix_2d.h"

#include "gtest/gtest.h"
#include "glog/logging.h"


class MRBlockReceiverTestV2 : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA256::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

    static std::unordered_map<int, int> generateConfig(int regionCount, int offset) {
        std::unordered_map<int, int> ret;
        for(int i=0; i<regionCount; i++) {
            ret[i] = offset+i;
        }
        return ret;
    }

};

TEST_F(MRBlockReceiverTestV2, TestBlockSignValidate) {
    auto bfgUtils = std::make_unique<tests::BFGUtils>();

    // 3 regions, each region 4 nodes+1 sender
    bfgUtils->addCFG(2, 2, 1, 4);
    bfgUtils->addCFG(2, 2, 1, 4);
    bfgUtils->addCFG(2, 2, 1, 4);

    // Init bfg
    std::shared_ptr<util::thread_pool_light> tpForBFGAndBCCSP(new util::thread_pool_light());
    auto bfg = std::make_shared<peer::BlockFragmentGenerator>(bfgUtils->cfgList, tpForBFGAndBCCSP.get());
    // Init bccsp
    auto bccsp = std::make_shared<util::BCCSP>(std::make_unique<util::DefaultKeyStorage>());
    // Bccsp prepare keys
    for (int i=0; i<3; i++) {
        for(int j=0; j<4; j++) {
            bccsp->generateED25519Key(std::to_string(i) + "_" + std::to_string(j), false);
        }
    }
    // Init storage
    auto storage = std::make_shared<peer::MRBlockStorage>(3);   // 3 regions

    // The local node in the local domain and the corresponding remote domain node for receiving fragments
    auto& localNodeConfig = tests::ProtoBlockUtils::GenerateNodesConfig(0, 1, 0)[0]->nodeConfig;

    // local servers broadcast ports, local node id == 0 (multi-master)
    std::unordered_map<int, std::vector<peer::v2::BlockReceiver::ConfigPtr>> configMap;
    // receive and repeat from region 1
    configMap[1] = tests::ProtoBlockUtils::GenerateNodesConfig(0, 4, 100);
    // receive and repeat from region 2
    configMap[2] = tests::ProtoBlockUtils::GenerateNodesConfig(0, 4, 150);

    // 4-1 senders, repeat from 3-1 regions
    util::Matrix2D<std::unique_ptr<util::ZMQInstance>> mockLocalSender(4, 3);
    for (int i=1; i<4; i++) {   // connect to other peers in local regions
        for (int j=1; j<3; j++) {   // multi-master, receive from different regions
            auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(configMap[j][i]->port);
            ASSERT_TRUE(sender != nullptr) << "Create instance failed";
            mockLocalSender(i, j) = std::move(sender);
        }
    }

    // create and init mr
    auto mr = peer::v2::MRBlockReceiver::NewMRBlockReceiver(
            localNodeConfig,
            generateConfig(3, 51100),   // we do not use this one yet, anything is ok
            generateConfig(3, 51150),   // these port are used to receive from crossRegionSender (as server)
            configMap); // the ports are used to receive from mockLocalSender (as client)
    ASSERT_TRUE(mr != nullptr);

    mr->setBCCSPWithThreadPool(bccsp, tpForBFGAndBCCSP);
    mr->setStorage(storage);
    std::unordered_map<int, int> regionNodesCount;
    for (int i=0; i<3; i++) {
        regionNodesCount[i] = 4;
    }
    auto fragmentCfg = peer::v2::FragmentUtil::GenerateAllConfig(regionNodesCount, 0, 0);
    mr->setBFGWithConfig(bfg, fragmentCfg.first);

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
            block->metadata.consensusSignatures.push_back({ski, 0, *ret});
        }
        block->serializeToString(&regionBlockRaw[i]);
    }

    std::unordered_map<int, proto::BlockNumber> startAt;
    for(int i=0; i<4; i++) {
        startAt[i] = 0;
    }
    ASSERT_TRUE(mr->checkAndStartService(startAt)) << "can not start mr";

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // init 2 cross region sender (not this one)
    std::vector<std::unique_ptr<util::ReliableZmqClient>> crossRegionSender(3);
    for (int i=1; i<3; i++) {   // skip local region
        auto sender = util::ReliableZmqClient::NewPublishClient("127.0.0.1", 51150 + i);
        ASSERT_TRUE(sender != nullptr) << "Create instance failed";
        crossRegionSender[i] = std::move(sender);
    }

    // encode to fragments, and send them
    for (int i=1; i<3; i++) {   // skip local region
        auto context = bfg->getEmptyContext(bfgUtils->cfgList[i]);
        context->initWithMessage(regionBlockRaw[i]);
        std::vector<std::string> serializedFragment(4);
        for (int j = 0; j < 2; j++) {   // two byzantine servers
            proto::EncodeBlockFragment fragment{0, {}, {}, static_cast<uint32_t>(j * 1), static_cast<uint32_t>((j + 1) * 1), {}};
            fragment.size = regionBlockRaw[i].size();
            std::string msgBuf;
            CHECK(context->serializeFragments(fragment.start, fragment.end, msgBuf)) << "create fragment failed!";
            fragment.encodeMessage = msgBuf;    // string view
            fragment.root = context->getRoot();
            // serialize to string
            if(!fragment.serializeToString(&serializedFragment[j], 0, true)) {
                CHECK(false) << "Encode message fragment failed!";
            }
        }
        // remote send
        crossRegionSender[i]->send(std::move(serializedFragment[0]));
        // two byzantine servers, local broadcast
        // x is the node id, y is the group id!
        mockLocalSender(1, i)->send(std::move(serializedFragment[1]));
    }

    for (int i=1; i<3; i++) {
        auto recBlock = storage->waitForBlock(i, 0);
        ASSERT_TRUE(*recBlock->getSerializedMessage() == regionBlockRaw[i]);
    }

    for (int i=1; i<3; i++) {
        auto maxId = storage->getMaxStoredBlockNumber(i);
        ASSERT_TRUE(maxId == 0) << "store block failed, id: " << maxId;
    }
}