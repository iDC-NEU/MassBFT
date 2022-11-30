//
// Created by peng on 11/30/22.
//


#include "peer/block_fragment_generator.h"

#include "gtest/gtest.h"
#include "glog/logging.h"



class BFGTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static void fillDummy(std::string& dummyBytes, int len) {
        dummyBytes.resize(len);
        for (auto &b: dummyBytes) {
            b = (char)(random() % 256);
        }
    }
};

TEST_F(BFGTest, IntrgrateTest) {
    // crypto and message pre-allocate
    util::OpenSSLSHA256::initCrypto();

    std::vector<peer::BlockFragmentGenerator::Config> cfgList;
    cfgList.push_back({
        .dataShardCnt=11,
        .parityShardCnt=22,
    });
    // create new instance with 10 ec workers per config
    peer::BlockFragmentGenerator bfg(cfgList, 10);
    std::string message;
    fillDummy(message, 1024*1024*5);

    for(int i=0; i<100; i++) {
        // get an ec worker with config
        auto context = bfg.getEmptyContext(cfgList[0]);
        context->initWithMessage(message);
        auto seg1 = context->serializeFragments(0, 4);
        ASSERT_TRUE(seg1 != std::nullopt);
        auto seg2 = context->serializeFragments(4, 8);
        ASSERT_TRUE(seg2 != std::nullopt);
        auto seg3 = context->serializeFragments(8, 12);
        ASSERT_TRUE(seg3 != std::nullopt);
        auto seg4 = context->serializeFragments(12, 16);
        ASSERT_TRUE(seg4 != std::nullopt);
        auto seg5 = context->serializeFragments(16, 19);    // 3
        ASSERT_TRUE(seg5 != std::nullopt);
        auto root = context->getRoot(); // pmt::hashString

        // get another worker for re-construct
        auto contextReconstruct = bfg.getEmptyContext(cfgList[0]);
        // 3+4+4=11
        ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, *seg4, 12, 16));
        ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, *seg2, 4, 8));
        ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, *seg5, 16, 19));

        auto msgRegenRet = contextReconstruct->regenerateMessage();
        ASSERT_TRUE(msgRegenRet);
        ASSERT_TRUE(msgRegenRet->substr(0, message.size()) == message) << msgRegenRet->substr(0, 100) << " vs " << message.substr(0, 100);
        bfg.freeContext(std::move(contextReconstruct));
        bfg.freeContext(std::move(context));
    }

}