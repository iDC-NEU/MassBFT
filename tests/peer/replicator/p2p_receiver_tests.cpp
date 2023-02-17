//
// Created by peng on 2/16/23.
//

#include "peer/replicator/p2p_receiver.h"
#include "tests/block_fragment_generator_utils.h"

#include "common/cv_wrapper.h"
#include "bthread/countdown_event.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class P2PReceiverTest : public ::testing::Test {
public:
    P2PReceiverTest() {
        bfgUtils.addCFG(4, 4, 1, 2);
        bfgUtils.startBFG();
    }

protected:
    void SetUp() override {
        context = bfgUtils.getContext(0);
    };

    void TearDown() override {
        context.reset();    // manually recycle
    };

    std::string generateMockFragment(proto::BlockNumber number, uint32_t start, uint32_t end) {
        return bfgUtils.generateMockFragment(context.get(), number, start, end);
    }

protected:
    tests::BFGUtils bfgUtils;
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

    std::unique_ptr<peer::P2PReceiver::FragmentBlock> block;
    bthread::CountdownEvent event(1);

    peer::P2PReceiver p2pReceiver;
    p2pReceiver.setOnMapUpdate([&](auto, auto b){
        // performance issues, set the actual data outside the cv.
        block = std::move(b);
        event.signal();
    });
    p2pReceiver.start(std::move(receiver));
    sender->send(std::move(msg));
    event.wait();
    ASSERT_TRUE(block != nullptr) << "Can not get block fragments";
    // TODO: validate fragment
}