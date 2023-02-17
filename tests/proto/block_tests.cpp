//
// Created by peng on 2/17/23.
//

#include "proto/block.h"

#include "gtest/gtest.h"

class BlockTest : public ::testing::Test {
protected:
    void SetUp() override {

    };

    void TearDown() override {

    };

};

TEST_F(BlockTest, CreateTest) {
    proto::Block b;
    b.header.dataHash = {"dataHash"};
    b.header.previousHash = {"previousHash"};
    b.header.number = 10;
    std::vector<std::string> rwSets;
    rwSets.emplace_back("rwSet1");
    rwSets.emplace_back("rwSet2");
    b.executeResult.setRWSets(std::move(rwSets));
    b.executeResult.transactionFilter.resize(2);
    b.executeResult.transactionFilter[1] = (std::byte)10;
    proto::SignatureString sig1 = {std::make_shared<std::string>("public key1"), {"sig1"}};
    proto::SignatureString sig2 = {std::make_shared<std::string>("public key2"), {"sig2"}};
    b.metadata.consensusSignatures.emplace_back(sig1);
    b.metadata.consensusSignatures.emplace_back(sig2);

    proto::SignatureString sig3 = {std::make_shared<std::string>("public key3"), {"sig3"}};
    proto::SignatureString sig4 = {std::make_shared<std::string>("public key4"), {"sig4"}};
    b.metadata.validateSignatures.emplace_back(sig3);
    b.metadata.validateSignatures.emplace_back(sig4);

    proto::SignatureString sig5 = {std::make_shared<std::string>("public key4"), {"sig4"}};
    proto::Block::Body::Envelop env1;
    env1.signature = sig5;
    std::string payload("payload for sig5");
    env1.setPayload(std::move(payload));
    b.body.userRequests.push_back(std::move(env1));

    // NOTE: ALL pointer must be not null!
    std::string message1, message2, message3;
    b.serializeToString(&message1);
    b.serializeToString(&message2);
    auto c = proto::Block::DeserializeBlock(std::move(message1));
    c->serializeToString(&message3);
    ASSERT_TRUE(message2 == message3);
}