//
// Created by peng on 11/30/22.
//


#include "peer/block_fragment_generator.h"
#include "common/timer.h"

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
                              .instanceCount = 2,
                              .concurrency = 2,
                      });
    auto tp = std::make_unique<util::thread_pool_light>();
    // create new instance with 10 ec workers per config
    peer::BlockFragmentGenerator bfg(cfgList, tp.get());
    std::string message, messageOut;
    fillDummy(message, 1024*1024*2);
    std::vector<std::string> segList(6);

    try {
        for (int i = 0; i < 100; i++) {
            // get an ec worker with config
            auto context = bfg.getEmptyContext(cfgList[0]);
            context->initWithMessage(message);
            ASSERT_TRUE(context->serializeFragments(0, 4, segList[0]));
            ASSERT_TRUE(context->serializeFragments(4, 8, segList[1]));
            ASSERT_TRUE(context->serializeFragments(8, 12, segList[2]));
            ASSERT_TRUE(context->serializeFragments(12, 16, segList[3]));
            ASSERT_TRUE(context->serializeFragments(16, 19, segList[4]));    // 3
            ASSERT_TRUE(context->serializeFragments(14, 18, segList[5]));    // special
            auto root = context->getRoot(); // pmt::hashString

            // get another worker for re-construct
            auto contextReconstruct = bfg.getEmptyContext(cfgList[0]);
            // 3+4+4=11
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root,  segList[3], 12, 16));
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[1], 4, 8));
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[5], 14, 18));
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[4], 16, 19));

            ASSERT_TRUE(contextReconstruct->regenerateMessage((int)message.size(), messageOut));
            ASSERT_TRUE(messageOut == message) << messageOut.substr(0, 100) << " vs " << message.substr(0, 100);
            bfg.freeContext(std::move(contextReconstruct));
            bfg.freeContext(std::move(context));
        }
    } catch (const std::exception& e) {
        ASSERT_TRUE(false) << e.what();
    }

}

TEST_F(BFGTest, IntrgrateTestParallel) {
    // crypto and message pre-allocate
    util::OpenSSLSHA256::initCrypto();

    std::vector<peer::BlockFragmentGenerator::Config> cfgList;
    cfgList.push_back({
                              .dataShardCnt=11,
                              .parityShardCnt=22,
                              .instanceCount = 2,
                              .concurrency = 2,
                      });
    auto tp = std::make_unique<util::thread_pool_light>();
    // create new instance with 10 ec workers per config
    peer::BlockFragmentGenerator bfg(cfgList, tp.get());
    std::string message, messageOut;
    fillDummy(message, 1024*1024*2);
    std::vector<std::string> segList(6);
    auto sema = util::NewSema();
    util::Timer timer;

    for(int i=0; i<100; i++) {
        // get an ec worker with config
        auto context = bfg.getEmptyContext(cfgList[0]);
        context->initWithMessage(message);
        tp->push_task([&] {
            ASSERT_TRUE(context->serializeFragments(0, 4, segList[0]));
            sema.signal();
        });
        tp->push_task([&] {
            ASSERT_TRUE(context->serializeFragments(4, 8, segList[1]));
            sema.signal();
        });
        tp->push_task([&] {
            ASSERT_TRUE(context->serializeFragments(8, 12, segList[2]));
            sema.signal();
        });
        tp->push_task([&] {
            ASSERT_TRUE(context->serializeFragments(12, 16, segList[3]));
            sema.signal();
        });
        tp->push_task([&] {
            ASSERT_TRUE(context->serializeFragments(16, 19, segList[4]));
            sema.signal();
        });
        auto root = context->getRoot(); // pmt::hashString

        // get another worker for re-construct
        auto contextReconstruct = bfg.getEmptyContext(cfgList[0]);
        util::wait_for_sema(sema, 5);

        // 3+4+4=11
        tp->push_task([&] {
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[3], 12, 16));
            sema.signal();
        });

        tp->push_task([&] {
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[1], 4, 8));
            sema.signal();
        });

        tp->push_task([&] {
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[4], 16, 19));
            sema.signal();
        });

        tp->push_task([&] {
            ASSERT_TRUE(contextReconstruct->validateAndDeserializeFragments(root, segList[2], 8, 12));
            sema.signal();
        });
        util::wait_for_sema(sema, 4);

        ASSERT_TRUE(contextReconstruct->regenerateMessage((int)message.size(), messageOut));
        ASSERT_TRUE(messageOut == message) << messageOut.substr(0, 100) << " vs " << message.substr(0, 100);
        bfg.freeContext(std::move(contextReconstruct));
        bfg.freeContext(std::move(context));
    }
    LOG(INFO) << "Total time: " << timer.end();
    LOG(INFO) << "Ratio: " << (double)segList[0].size()/4*33/(int)message.size();
}