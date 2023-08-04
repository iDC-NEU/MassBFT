//
// Created by peng on 11/6/22.
//

#include "ycsb/sdk/client_sdk.h"
#include "tests/mock_property_generator.h"
#include "tests/peer/mock_peer.h"
#include "gtest/gtest.h"
#include "common/proof_generator.h"

class ClientSDKTest : public ::testing::Test {
protected:
    void SetUp() override {
        tests::MockPropertyGenerator::GenerateDefaultProperties(1, 3);
        tests::MockPropertyGenerator::SetLocalId(0, 0);
        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 2);
        ycsb::sdk::ClientSDK::InitSDKDependencies();
    };

    void TearDown() override { };
};

TEST_F(ClientSDKTest, BasicTest) {
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, false, false);
    auto clientSDK = ycsb::sdk::ClientSDK::NewSDK(*p);
    ASSERT_TRUE(clientSDK);
    ASSERT_TRUE(clientSDK->connect());
    ycsb::sdk::SendInterface* sender = clientSDK.get();
    ycsb::sdk::ReceiveInterface* receiver = clientSDK.get();
    // wait until server ready
    std::this_thread::sleep_for(std::chrono::seconds(1));
    ASSERT_TRUE(receiver->getChainHeight(0, 100) == -1);
    auto ret1 = sender->invokeChaincode("ycsb", "w", "args1");
    ASSERT_TRUE(ret1);
    auto ret2 = sender->invokeChaincode("ycsb", "w", "args2");
    ASSERT_TRUE(ret2);
    auto block = receiver->getBlock(0, 0, 1000);
    ASSERT_TRUE(block);
    ASSERT_TRUE(block->body.userRequests.size() == 2);
    ASSERT_TRUE(receiver->getChainHeight(0, 100) == 0);
    auto ret3 = receiver->getTransaction(block->body.userRequests[0]->getSignature().digest, 0, 0, 1000);
    ASSERT_TRUE(ret3);
    auto ret4 = receiver->getTransaction(block->body.userRequests[0]->getSignature().digest, 0, 0, 1000);
    ASSERT_TRUE(ret4);

    util::ProofGenerator pg(block->body);
    ASSERT_TRUE(receiver->ValidateMerkleProof(pg.generateMerkleTree()->getRoot(),
                                              ret4->envelopProof,
                                              *ret4->envelop->getSerializedMessage()));
    auto header = receiver->getBlockHeader(0, 0, -1);
    ASSERT_TRUE(header != nullptr && header->header.number == 0);
}
