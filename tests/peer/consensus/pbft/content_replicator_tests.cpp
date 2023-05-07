//
// Created by user on 23-3-21.
//

#include "peer/consensus/pbft/content_replicator.h"
#include "common/pbft/pbft_rpc_service.h"
#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class ConsensusReplicatorTest : public ::testing::Test {
public:
    ConsensusReplicatorTest() {
        util::OpenSSLED25519::initCrypto();
        localNodes = tests::ProtoBlockUtils::GenerateNodesConfig(0, 4, 1000);
        auto ms = std::make_unique<util::DefaultKeyStorage>();
        bccsp = std::make_shared<util::BCCSP>(std::move(ms));
        threadPool = std::make_unique<util::thread_pool_light>(4);  // 4 threads
        for (const auto& it: localNodes) {
            auto key = bccsp->generateED25519Key(it->nodeConfig->ski, false);
            CHECK(key != nullptr);
            auto sm = new peer::consensus::ContentReplicator(localNodes, it->nodeConfig->nodeId);
            sm->setBCCSPWithThreadPool(bccsp, threadPool);
            sm->checkAndStart();
            stateMachines.emplace_back(sm);
        }
    }
protected:
    void SetUp() override {

    };

    void TearDown() override {
    };

    // must NOT return nullptr
    auto createSignedEnvelop(int nodeId) {
        auto& ski = localNodes[nodeId]->nodeConfig->ski;
        std::unique_ptr<proto::Envelop> envelop(new proto::Envelop());
        proto::SignatureString sig = {
                ski,
                std::make_shared<std::string>()};
        auto key = bccsp->GetKey(ski);
        CHECK(key->Private());
        std::string payload("payload for an envelop" + std::to_string(rand()));
        auto ret = key->Sign(payload.data(), payload.size());
        CHECK(ret != std::nullopt);
        sig.digest = *ret;
        envelop->setPayload(std::move(payload));
        envelop->setSignature(std::move(sig));
        return envelop;
    }

    void prepareBatches(peer::consensus::ContentReplicator* sm, int count) {
        for (int round =0; round<count; round++) {
            std::vector<std::unique_ptr<proto::Envelop>> batch;
            for (int i=0; i<200; i++) { // assume 4 nodes
                batch.push_back(createSignedEnvelop(i%4));
            }
            sm->pushUnorderedBlock<false>(std::move(batch));
        }
    }

    std::vector<std::shared_ptr<util::ZMQInstanceConfig>> localNodes;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<util::thread_pool_light> threadPool;
    std::vector<std::shared_ptr<util::pbft::PBFTStateMachine>> stateMachines;
};

TEST_F(ConsensusReplicatorTest, TestStateMachineNormalCase) {
    auto& sm = stateMachines.front();
    auto& nodeConfig = localNodes.front()->nodeConfig;
    // Leader is not starting
    auto ret = sm->OnRequestProposal(nodeConfig, 5, "placeholder");
    ASSERT_TRUE(ret == std::nullopt);
    sm->OnLeaderChange(nodeConfig, localNodes[1]->nodeConfig, 11);
    // insert some user request batches
    auto child = dynamic_cast<peer::consensus::ContentReplicator*>(sm.get());
    ASSERT_TRUE(child != nullptr);
    prepareBatches(child, 10);
    // start the loader and request some batches
    sm->OnLeaderStart(nodeConfig, 12);
    auto serializedBlockHeader = sm->OnRequestProposal(nodeConfig, 12, "placeholder");
    ASSERT_TRUE(serializedBlockHeader != std::nullopt);
    for (int i=1; i<(int)stateMachines.size(); i++) {
        // check if all other servers receive the block
        auto res = stateMachines[i]->OnVerifyProposal(localNodes[i]->nodeConfig, *serializedBlockHeader);
        ASSERT_TRUE(res == true);
    }
    // after all follower received the block, deliver it
    for (int i=0; i<(int)stateMachines.size(); i++) {
        stateMachines[i]->OnDeliver(localNodes[i]->nodeConfig, *serializedBlockHeader, {});
    }
}


TEST_F(ConsensusReplicatorTest, TestWithPBFTService) {
    util::OpenSSLED25519::initCrypto();
    std::vector<::util::NodeConfigPtr> nodes(localNodes.size());
    for (auto& it: localNodes) {
        nodes[it->nodeConfig->nodeId] = it->nodeConfig;
    }
    for (int i=0; i<4; i++) {
        auto service = std::make_unique<util::pbft::PBFTRPCService>();
        CHECK(service->checkAndStart(nodes, bccsp, stateMachines[i]));
        if (util::DefaultRpcServer::AddService(service.release(), 9510 + i) != 0) {
            CHECK(false) << "Fail to add globalControlService!";
        }
        util::DefaultRpcServer::Start(9510 + i);
    }
    sleep(25);
    for (int i=0; i<200000; i++) {
        for (auto& it: stateMachines) {
            // insert some user request batches
            auto child = dynamic_cast<peer::consensus::ContentReplicator*>(it.get());
            ASSERT_TRUE(child != nullptr);
            prepareBatches(child, 1);
        }
    }
    sleep(3600);
}
