//
// Created by user on 23-8-3.
//

#include "common/proof_generator.h"

#include "tests/proto_block_utils.h"
#include "gtest/gtest.h"

class ProofGeneratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA1::initCrypto();
        util::OpenSSLSHA256::initCrypto();
    };

    void TearDown() override {
    };
};

TEST_F(ProofGeneratorTest, TestGenerateBodyProof) {
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    auto mt = util::UserRequestMTGenerator::GenerateMerkleTree(block->body.userRequests, nullptr);
    ASSERT_TRUE(mt != nullptr);
    LOG(INFO) << "Root hash: " << util::OpenSSLSHA256::toString(mt->getRoot());
}

TEST_F(ProofGeneratorTest, TestGenerateExecResultProof) {
    auto block = tests::ProtoBlockUtils::CreateDemoBlock();
    auto mt = util::ExecResultMTGenerator::GenerateMerkleTree(block->executeResult.txReadWriteSet,
                                                              block->executeResult.transactionFilter);
    ASSERT_TRUE(mt != nullptr);
    LOG(INFO) << "Root hash: " << util::OpenSSLSHA256::toString(mt->getRoot());
}