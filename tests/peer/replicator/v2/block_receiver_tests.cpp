//
// Created by user on 23-3-11.
//

#include "peer/replicator/v2/block_receiver.h"
#include "tests/block_fragment_generator_utils.h"
#include "tests/proto_block_utils.h"
#include "common/matrix_2d.h"

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "peer/replicator/v2/fragment_util.h"

class FragmentReceiverTestV2 : public ::testing::Test {
public:
    FragmentReceiverTestV2() {
        bfgUtils.addCFG(4, 4, 1, 2);
        bfgUtils.startBFG();
    }

protected:
    void SetUp() override {
        context = bfgUtils.getContext(0);
    };

    void TearDown() override {
        context.reset();    // manually recycle
    };

    std::string generateMockFragment(proto::BlockNumber number, uint32_t start, uint32_t end) {
        return bfgUtils.generateMockFragment(context.get(), number, start, end);
    }

protected:
    tests::BFGUtils bfgUtils;
    std::shared_ptr<peer::BlockFragmentGenerator::Context> context;
};

TEST_F(FragmentReceiverTestV2, TestBlockSignValidate) {
    proto::BlockNumber bkNum = 10;
    auto msg = this->generateMockFragment(bkNum, 0, 4);
    auto receiver = util::ZMQInstance::NewClient<zmq::socket_type::sub>("127.0.0.1", 51200);
    ASSERT_TRUE(receiver != nullptr) << "Create instance failed";

    auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200);
    ASSERT_TRUE(sender != nullptr) << "Create instance failed";

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::unique_ptr<peer::v2::FragmentBlock> block;
    bthread::CountdownEvent event(1);

    peer::v2::LocalFragmentReceiver fragmentReceiver;
    fragmentReceiver.setOnReceived([&](auto, auto b) {
        // performance issues, set the actual data outside the cv.
        block = std::move(b);
        event.signal();
    });
    ASSERT_TRUE(fragmentReceiver.checkAndStart(std::move(receiver)));
    sender->send(std::move(msg));
    event.wait();
    ASSERT_TRUE(block != nullptr) << "Can not get block fragments";
}


class BlockReceiverTestV2 : public ::testing::Test {
public:
    BlockReceiverTestV2() {
        dataShardCnt = 4;
        parityShardCnt = 8;
        cfgPosition = -1;
        refreshConfig();
    }

    void refreshConfig() {
        bfgUtils.cfgList.clear();
        // Each zone must guarantee at least one configuration
        // Pushing more configs is good for concurrency
        // We only have 1 region here, so size of the config is set to 5 (4 region instance+1 sender instance)
        cfgPosition = bfgUtils.addCFG(dataShardCnt, parityShardCnt, 1, 5);
        bfgUtils.startBFG();
    }

protected:
    void SetUp() override {
        util::OpenSSLSHA256::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

protected:
    tests::BFGUtils bfgUtils;
    int dataShardCnt;
    int parityShardCnt;
    int cfgPosition;
};

TEST_F(BlockReceiverTestV2, IntrgrateTest) {
    // region 0
    int regionId = 0;
    int nodesPerRegion = 4;
    int blockNumber = 10;
    // prepare all fragments
    std::vector<std::unique_ptr<util::ZMQInstance>> servers(nodesPerRegion);
    for (int i = 1; i < (int) servers.size(); i++) {    // skip localId=0 sender.
        auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200 + i);
        ASSERT_TRUE(sender != nullptr) << "Create instance failed";
        servers[i] = std::move(sender);
    }

    int shardPerNode = (parityShardCnt + dataShardCnt) / nodesPerRegion;    // must be divisible
    // spin up client
    auto configList = tests::ProtoBlockUtils::GenerateNodesConfig(regionId, nodesPerRegion, 0);
    auto localNodeConfig = tests::ProtoBlockUtils::GenerateNodesConfig(1, 1, (int)configList.size())[0]->nodeConfig;

    // connect to remote region==regionId
    auto regionZeroReceiver = peer::v2::BlockReceiver::NewBlockReceiver(localNodeConfig, 51199, configList, 51198, 0);
    ASSERT_TRUE(regionZeroReceiver != nullptr) << "Create instance failed";
    regionZeroReceiver->setBFG(bfgUtils.bfg);
    regionZeroReceiver->setBFGConfig(bfgUtils.cfgList[regionId]);
    regionZeroReceiver->activeStart(blockNumber);

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto senderContext = bfgUtils.getContext(cfgPosition);
    std::vector<std::string> serializedFragment(nodesPerRegion);
    for (int i = 0; i < (int) servers.size(); i++) {
        serializedFragment[i] = bfgUtils.generateMockFragment(senderContext.get(), blockNumber, i * shardPerNode, (i + 1) * shardPerNode);
    }
    // send fragment to corresponding peer
    //    for (int i = 1; i < (int) servers.size(); i++) {
    //        servers[i]->send(std::move(serializedFragment[i]));
    //    }
    // we only let server 0 and 1 send the fragment
    servers[1]->send(std::move(serializedFragment[1]));
    auto remoteSender = util::ReliableZmqClient::NewPublishClient("127.0.0.1", 51199);
    remoteSender->send(std::move(serializedFragment[0]));

    // check the received fragment
    auto ret = regionZeroReceiver->activeGet();
    ASSERT_TRUE(ret != nullptr) << "Can not get block fragments!";
    if (*ret != bfgUtils.message) {
        LOG(INFO) << ret->substr(0, 100);
        LOG(INFO) << bfgUtils.message.substr(0, 100);
    }
    ASSERT_TRUE(*ret == bfgUtils.message) << "Message mismatch!";
}

// test if there is a resource leak
TEST_F(BlockReceiverTestV2, ContinueousSending) {
    // region 0
    int regionId = 0;
    int nodesPerRegion = 4;
    int startWith = 10;
    int endWith = 1000;
    // prepare all fragments
    std::vector<std::unique_ptr<util::ZMQInstance>> servers(nodesPerRegion);
    for (int i = 1; i < (int) servers.size(); i++) {
        auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200 + i);
        ASSERT_TRUE(sender != nullptr) << "Create instance failed";
        servers[i] = std::move(sender);
    }
    int shardPerNode = (parityShardCnt + dataShardCnt) / nodesPerRegion;    // must be divisible
    // spin up client
    auto configList = tests::ProtoBlockUtils::GenerateNodesConfig(regionId, nodesPerRegion, 0);
    auto localNodeConfig = tests::ProtoBlockUtils::GenerateNodesConfig(1, 1, (int)configList.size())[0]->nodeConfig;

    auto regionZeroReceiver = peer::v2::BlockReceiver::NewBlockReceiver(localNodeConfig, 51199, configList, 51198, 0);
    ASSERT_TRUE(regionZeroReceiver != nullptr) << "Create instance failed";
    regionZeroReceiver->setBFG(bfgUtils.bfg);
    regionZeroReceiver->setBFGConfig(bfgUtils.cfgList[regionId]);
    regionZeroReceiver->passiveStart(startWith);

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto remoteSender = util::ReliableZmqClient::NewPublishClient("127.0.0.1", 51199);
    for (int blockNumber = startWith; blockNumber < endWith; blockNumber++) {
        LOG(INFO) << "Block number " << blockNumber << " start sync sending.";
        tests::BFGUtils::FillDummy(bfgUtils.message, 1024 * 2);
        auto senderContext = bfgUtils.getContext(cfgPosition);
        std::vector<std::string> serializedFragment(nodesPerRegion);
        for (int i = 0; i < (int) servers.size(); i++) {
            serializedFragment[i] = bfgUtils.generateMockFragment(senderContext.get(), blockNumber, i * shardPerNode, (i + 1) * shardPerNode);
        }
        // send fragment to corresponding peer
        //        for (int i = 1; i < (int) servers.size(); i++) {
        //            servers[i]->send(std::move(serializedFragment[i]));
        //        }
        // we only let server 0 and 1 send the fragment
        servers[1]->send(std::move(serializedFragment[1]));
        remoteSender->send(std::move(serializedFragment[0]));

        // check the received fragment
        auto ret = regionZeroReceiver->passiveGet(blockNumber);
        ASSERT_TRUE(ret != nullptr) << "Can not get block fragments!";
        if (*ret != bfgUtils.message) {
            LOG(INFO) << ret->substr(0, 100);
            LOG(INFO) << bfgUtils.message.substr(0, 100);
        }
        ASSERT_TRUE(*ret == bfgUtils.message) << "Message mismatch!";
    }
}

// test performance
TEST_F(BlockReceiverTestV2, SendingAsync) {
    // region 0
    int regionId = 0;
    int nodesPerRegion = 12;
    int startWith = 0;
    int endWith = 100;
    dataShardCnt = nodesPerRegion;
    parityShardCnt = nodesPerRegion * 2;
    refreshConfig();
    // prepare all fragments
    std::vector<std::unique_ptr<util::ZMQInstance>> servers(nodesPerRegion);
    for (int i = 1; i < (int) servers.size(); i++) {
        auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200 + i);
        ASSERT_TRUE(sender != nullptr) << "Create instance failed";
        servers[i] = std::move(sender);
    }
    int shardPerNode = (parityShardCnt + dataShardCnt) / nodesPerRegion;    // must be divisible
    // spin up client
    auto configList = tests::ProtoBlockUtils::GenerateNodesConfig(regionId, nodesPerRegion, 0);
    auto localNodeConfig = tests::ProtoBlockUtils::GenerateNodesConfig(1, 1, (int)configList.size())[0]->nodeConfig;

    auto regionZeroReceiver = peer::v2::BlockReceiver::NewBlockReceiver(localNodeConfig, 51199, configList, 51198, 0);
    ASSERT_TRUE(regionZeroReceiver != nullptr) << "Create instance failed";
    regionZeroReceiver->setBFG(bfgUtils.bfg);
    regionZeroReceiver->setBFGConfig(bfgUtils.cfgList[regionId]);
    regionZeroReceiver->activeStart(startWith);

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::vector<std::string> msgList(endWith - startWith);
    for (int blockNumber = startWith; blockNumber < endWith; blockNumber++) {
        tests::BFGUtils::FillDummy(msgList[blockNumber - startWith], 250 * 1024);
    }
    auto remoteSender = util::ReliableZmqClient::NewPublishClient("127.0.0.1", 51199);

    std::unique_ptr<util::thread_pool_light> threadPool(new util::thread_pool_light(4));
    auto f1 = threadPool->submit([&] {
        for (int blockNumber = startWith; blockNumber < endWith; blockNumber++) {
            LOG(INFO) << "Block number " << blockNumber << " start sending.";
            bfgUtils.message = msgList[blockNumber - startWith];
            auto senderContext = bfgUtils.getContext(cfgPosition);
            std::vector<std::string> serializedFragment(nodesPerRegion);
            bthread::CountdownEvent countdown((int) servers.size());
            for (int i = 0; i < (int) servers.size(); i++) {
                threadPool->push_task([&, i = i] {
                    serializedFragment[i] = bfgUtils.generateMockFragment(senderContext.get(), blockNumber, i * shardPerNode, (i + 1) * shardPerNode);
                    countdown.signal();
                });
            }
            countdown.wait();
            // send fragment to corresponding peer
            //        for (int i = 1; i < (int) servers.size(); i++) {
            //            servers[i]->send(std::move(serializedFragment[i]));
            //        }
            // we only let server 0 and 1 send the fragment
            // byzantine nodes count
            auto byzantineNodes = int(nodesPerRegion/3)*2;
            for (int i = 1; i < nodesPerRegion - byzantineNodes; i++) {
                servers[i]->send(std::move(serializedFragment[i]));
            }
            remoteSender->send(std::move(serializedFragment[0]));
        }
    });

    for (int blockNumber = startWith; blockNumber < endWith; blockNumber++) {
        LOG(INFO) << "Block number " << blockNumber << " start receiving.";
        // check the received fragment
        auto ret = regionZeroReceiver->activeGet();
        ASSERT_TRUE(ret != nullptr) << "Can not get block fragments!";
        auto &msg = msgList[blockNumber - startWith];
        if (*ret != msg) {
            LOG(INFO) << ret->substr(0, 100);
            LOG(INFO) << msg.substr(0, 100);
        }
        ASSERT_TRUE(*ret == msg) << "Message mismatch!";
    }
    // join the thread
    f1.wait();
    LOG(INFO) << "Exit.";
}