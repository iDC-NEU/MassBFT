//
// Created by peng on 2/16/23.
//

#include <memory>

#include "peer/replicator/p2p_receiver.h"
#include "peer/block_fragment_generator.h"

#include "common/cv_wrapper.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class P2PReceiverTest : public ::testing::Test {
public:
    P2PReceiverTest() {
        fillDummy(message, 1024*1024*2);
        tp = std::make_unique<util::thread_pool_light>();
        cfg = { .dataShardCnt=4,
                .parityShardCnt=4,
                .instanceCount=1,
                .concurrency=2, };
    }

protected:
    void SetUp() override {
        // crypto and message pre-allocate
        util::OpenSSLSHA256::initCrypto();
        std::vector<peer::BlockFragmentGenerator::Config> cfgList = {cfg};
        bfg = std::make_unique<peer::BlockFragmentGenerator>(cfgList, tp.get());
        context = bfg->getEmptyContext(cfg);
        context->initWithMessage(message);
    };

    void TearDown() override {
        context.reset();    // manually recycle
    };

    std::string generateMockFragment(proto::BlockNumber number, uint32_t start, uint32_t end) {
        proto::EncodeBlockFragment fragment{number, {}, start, end, {}};
        auto encodeMessageBuf = fillFragment(start, end);
        fragment.encodeMessage = encodeMessageBuf;
        std::string dataOut;
        zpp::bits::out out(dataOut);
        if(failure(out(fragment))) {
            CHECK(false) << "Encode message fragment failed!";
        }
        return dataOut;
    }

private:
    std::string fillFragment(uint32_t start, uint32_t end) {
        std::string buffer;
        CHECK(context->serializeFragments(start, end, buffer)) << "create fragment failed!";
        return buffer;
    }

    static void fillDummy(std::string& dummyBytes, int len) {
        dummyBytes.resize(len);
        for (auto &b: dummyBytes) {
            b = (char)(random() % 256);
        }
    }

protected:
    std::string message;
    std::unique_ptr<util::thread_pool_light> tp;
    peer::BlockFragmentGenerator::Config cfg;
    std::unique_ptr<peer::BlockFragmentGenerator> bfg;
    std::shared_ptr<peer::BlockFragmentGenerator::Context> context;
};

TEST_F(P2PReceiverTest, IntrgrateTest) {
    proto::BlockNumber bkNum = 10;
    auto msg = this->generateMockFragment(bkNum, 0, 4);
    auto receiver = util::ZMQInstance::NewClient<zmq::socket_type::sub>("127.0.0.1", 51200);
    ASSERT_TRUE(receiver != nullptr) << "Create instance failed";

    auto sender = util::ZMQInstance::NewServer<zmq::socket_type::pub>(51200);
    ASSERT_TRUE(sender != nullptr) << "Create instance failed";

    // Give the subscribers a chance to connect, so they don't lose any messages
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    peer::P2PReceiver p2PReceiver(std::move(receiver));
    std::unordered_map<proto::BlockNumber, bool> bits;
    util::CVWrapper cv;
    p2PReceiver.setOnMapUpdate([&](auto n){
        cv.notify_all([&]{
            bits[n] = true;
        });
    });
    sender->send(std::move(msg));
    cv.wait([&] {
        return bits[bkNum];
    });
    auto ret = p2PReceiver.tryGet(bkNum);
    ASSERT_TRUE(ret != nullptr) << "Can not get block fragments";
    // TODO: validate fragment
}