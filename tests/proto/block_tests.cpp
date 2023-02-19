//
// Created by peng on 2/17/23.
//

#include "tests/proto_block_utils.h"

#include "gtest/gtest.h"

class BlockTest : public ::testing::Test {
protected:
    void SetUp() override {

    };

    void TearDown() override {

    };

};

TEST_F(BlockTest, CreateTest) {
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    // NOTE: ALL pointer must be not null!
    std::string message1, message2, message3;
    block->serializeToString(&message1);
    block->serializeToString(&message2);
    std::shared_ptr<proto::Block> c(new proto::Block);
    c->deserializeFromString(std::move(message1));
    c->serializeToString(&message3);
    ASSERT_TRUE(message2 == message3);
}

TEST_F(BlockTest, PositionTest) {
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    // test deserialize match the serialized message
    std::string body1, blockRaw;
    zpp::bits::out out(body1);
    ASSERT_TRUE(!failure(out(block->header, block->body)));
    ASSERT_TRUE(block->serializeToString(&blockRaw).valid);

    auto ret = block->deserializeFromString(std::move(blockRaw));
    ASSERT_TRUE(ret.valid);
    // header + body serialized data
    std::string_view serBody(block->getSerializedMessage()->data()+ret.headerPos, ret.execResultPos-ret.headerPos);

    ASSERT_TRUE(serBody == body1);
}

TEST_F(BlockTest, SerializePartTest) {
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    std::string raw1, raw2;
    block->serializeToString(&raw1);
    block->header.dataHash = {"updated"};
    block->serializeToString(&raw2);
    ASSERT_TRUE(proto::Block::UpdateSerializedHeader(block->header, &raw1, 0).valid);
    ASSERT_TRUE(raw1 == raw2);
    proto::SignatureString sig = {"ski", std::make_shared<std::string>("public key3"), {"sig3"}};
    block->metadata.validateSignatures.emplace_back(sig);
    auto position = block->serializeToString(&raw2);
    ASSERT_TRUE(proto::Block::AppendSerializedExecutionResult(*block, &raw1, position.execResultPos).valid);
    ASSERT_TRUE(raw1 == raw2);
}