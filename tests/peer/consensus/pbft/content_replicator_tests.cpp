//
// Created by user on 23-3-21.
//

#include "peer/consensus/pbft/content_replicator.h"
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
        std::string payload("payload for an envelop");
        auto ret = key->Sign(payload.data(), payload.size());
        CHECK(ret != std::nullopt);
        sig.digest = *ret;
        envelop->setPayload(std::move(payload));
        envelop->setSignature(std::move(sig));
        return envelop;
    }

    std::vector<std::shared_ptr<util::ZMQInstanceConfig>> localNodes;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<util::thread_pool_light> threadPool;
    std::vector<std::shared_ptr<peer::consensus::PBFTStateMachine>> stateMachines;
};

TEST_F(ConsensusReplicatorTest, TestStateMachineNormalCase) {
    auto& sm = stateMachines.front();
    auto& nodeConfig = localNodes.front()->nodeConfig;
    // Leader is not starting
    auto ret = sm->OnRequestProposal(nodeConfig, 5, "placeholder");
    ASSERT_TRUE(ret == std::nullopt);
    sm->OnLeaderStart(nodeConfig, 10);
    // Wrong sequence
    ret = sm->OnRequestProposal(nodeConfig, 5, "placeholder");
    ASSERT_TRUE(ret == std::nullopt);
    sm->OnLeaderStop(nodeConfig, 11);
    // insert some user request batches
    auto child = dynamic_cast<peer::consensus::ContentReplicator*>(sm.get());
    ASSERT_TRUE(child != nullptr);
    for (int round =0; round<10; round++) {
        std::vector<std::unique_ptr<proto::Envelop>> batch;
        for (int i=0; i<200; i++) { // assume 4 nodes
            batch.push_back(createSignedEnvelop(i%4));
        }
        child->pushUnorderedBlock(std::move(batch));
    }
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