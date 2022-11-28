//
// Created by peng on 11/28/22.
//

#include "common/zeromq.h"
#include "common/thread_pool_light.h"

#include "gtest/gtest.h"
#include "lightweightsemaphore.h"
#include <vector>
#include <thread>

class ZMQTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(ZMQTest, TestPubSub) {
    util::thread_pool_light tp(2);

    auto receiver = util::ZMQInstance::NewServer<zmq::socket_type::sub>(51200);
    ASSERT_TRUE(receiver != nullptr) << "Create instance failed";

    auto sender = util::ZMQInstance::NewClient<zmq::socket_type::pub>("127.0.0.1", 51200);
    ASSERT_TRUE(sender != nullptr) << "Create instance failed";

    auto signal = util::NewSema();

    auto f1 = tp.submit([&sender, &signal]{
        util::wait_for_sema(signal);
        // Give the subscribers a chance to connect, so they don't lose any messages
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        return sender->send(std::string("hello, world!"));
    });

    auto f2 = tp.submit([&receiver, &signal]{
        util::wait_for_sema(signal);
        auto ret = receiver->receive();
        if (!ret) {
            return false;
        }
        auto msg = std::move(*ret);
        return msg.to_string() == "hello, world!";
    });

    signal.signal(2);

    ASSERT_TRUE(f1.get()) << "Can not send msg!";
    ASSERT_TRUE(f2.get()) << "Receive invalid string!";

}

TEST_F(ZMQTest, MultiGetStore) {
    int strLen = 1024;  //1KB
    int cnt = 1000000;  //1M message

    // prepare message send
    std::vector<std::string> messageList;
    messageList.reserve(cnt);
    for(auto& msg: messageList) {
        msg.resize(strLen, '0');
    }

    std::vector<zmq::message_t> receiveList;
    receiveList.reserve(cnt);
}
