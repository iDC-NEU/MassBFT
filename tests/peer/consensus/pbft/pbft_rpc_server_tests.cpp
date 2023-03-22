//
// Created by user on 23-3-21.
//

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "peer/consensus/pbft/pbft_rpc_server.h"
#include "tests/proto_block_utils.h"


class MockPBFTStateMachine : public peer::consensus::PBFTStateMachine {
public:
    // Call by followers only
    bool OnVerifyProposal(::util::NodeConfigPtr localNode, std::unique_ptr<::proto::Block> block) override {
        LOG(INFO) << "OnVerifyProposal, Block number: " << block->header.number;
        return true;
    }

    bool OnDeliver(::util::NodeConfigPtr localNode, std::unique_ptr<::proto::Block> block) override {
        LOG(INFO) << "OnDeliver, Block number: " << block->header.number;
        return true;
    }

    void OnLeaderStart(::util::NodeConfigPtr localNode, const std::string& context) override {

    }

    void OnLeaderStop(::util::NodeConfigPtr localNode, const std::string& context) override {

    }

    // Call by the leader only
    std::shared_ptr<::proto::Block> OnRequestProposal(::util::NodeConfigPtr localNode, int blockNumber, const std::string& context) override {
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->header.number = nextBlockNumber;
        nextBlockNumber++;
        return block;
    }

private:
    int nextBlockNumber = 0;
};


class PBFTRPCServerTest : public ::testing::Test {
protected:
    void SetUp() override {
        init();
    };

    void TearDown() override {
    };

    void init() {
        auto ms = std::make_unique<util::DefaultKeyStorage>();
        bccsp = std::make_shared<util::BCCSP>(std::move(ms));
        for (int i=0; i<4; i++) {
            util::NodeConfigPtr cfg(new util::NodeConfig);
            cfg->nodeId = i;
            cfg->groupId = 0;
            cfg->ski = "0_" + std::to_string(i);
            cfg->ip = "127.0.0.1";
            auto key = bccsp->generateED25519Key(cfg->ski, false);
            CHECK(key != nullptr);
            localNodes[i] = std::move(cfg);
        }
        stateMachine = std::make_shared<MockPBFTStateMachine>();
    }

    std::unordered_map<int, ::util::NodeConfigPtr> localNodes;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<MockPBFTStateMachine> stateMachine;
};

TEST_F(PBFTRPCServerTest, TestStartServer) {
    util::OpenSSLED25519::initCrypto();
    auto service = std::make_unique<peer::consensus::PBFTRPCService>();
    CHECK(service->checkAndStart(localNodes, bccsp, stateMachine));
    util::MetaRpcServer::Start();
    sleep(3600);
}