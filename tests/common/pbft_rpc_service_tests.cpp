//
// Created by user on 23-3-21.
//

#include "common/pbft/mock_rpc_service.h"
#include "common/pbft/pbft_rpc_service.h"
#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class MockPBFTStateMachine : public util::pbft::PBFTStateMachine {
public:
    [[nodiscard]] std::optional<::util::OpenSSLED25519::digestType> OnSignMessage(const ::util::NodeConfigPtr&, const std::string&) const override {
        return std::nullopt;
    }
    // Call by followers only
    bool OnVerifyProposal(::util::NodeConfigPtr, const std::string&) override {
        return true;
    }

    bool OnDeliver(::util::NodeConfigPtr,
                   const std::string&,
                   std::vector<::proto::SignatureString>) override {
        return true;
    }

    void OnLeaderStart(::util::NodeConfigPtr, int) override {
    }

    void OnLeaderChange(::util::NodeConfigPtr, ::util::NodeConfigPtr, int) override {
    }

    // Call by the leader only
    std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr, int, const std::string&) override {
        auto block = tests::ProtoBlockUtils::CreateDemoBlock();
        block->header.number = nextBlockNumber;
        nextBlockNumber++;
        std::string buf;
        block->header.serializeToString(&buf);
        return buf;
    }

private:
    int nextBlockNumber = 0;
};


class PBFTRPCServiceTest : public ::testing::Test {
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
            localNodes.push_back(std::move(cfg));
        }
        stateMachine = std::make_shared<MockPBFTStateMachine>();
    }

    std::vector<::util::NodeConfigPtr> localNodes;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<MockPBFTStateMachine> stateMachine;
};

TEST_F(PBFTRPCServiceTest, TestPBFTRPCService) {
    util::OpenSSLED25519::initCrypto();
    auto service = std::make_unique<util::pbft::PBFTRPCService>();
    CHECK(service->checkAndStart(localNodes, bccsp, stateMachine));
    if (util::MetaRpcServer::AddService(service.release()) != 0) {
        CHECK(false) << "Fail to add globalControlService!";
    }
    util::MetaRpcServer::Start();
    sleep(3600);
}

TEST_F(PBFTRPCServiceTest, TestServiceInterface) {
    util::OpenSSLED25519::initCrypto();
    auto service = std::make_unique<util::pbft::MockRPCService>();
    CHECK(service->checkAndStart());
    util::MetaRpcServer::Start();
    sleep(3600);
}