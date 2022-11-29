//
// Created by peng on 11/28/22.
//

#include "common/zeromq.h"
#include "common/thread_pool_light.h"
#include "common/timer.h"

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

    util::thread_pool_light tp{2};
};

TEST_F(ZMQTest, TestPubSub) {
    auto receiver = util::ZMQInstance::NewServer<zmq::socket_type::sub>(51200);
    ASSERT_TRUE(receiver != nullptr) << "Create instance failed";

    auto sender = util::ZMQInstance::NewClient<zmq::socket_type::pub>("127.0.0.1", 51200);
    ASSERT_TRUE(sender != nullptr) << "Create instance failed";

    auto signal = util::NewSema();

    auto f1 = tp.submit([&sender, &signal]{
        // Give the subscribers a chance to connect, so they don't lose any messages
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        util::wait_for_sema(signal);
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
    int64_t strLen = 256;  // bytes
    int64_t cnt = 4*2*1024*1024;  // message cnt

    // prepare message send
    std::vector<std::string> messageList;
    messageList.resize(cnt);
    for(auto& msg: messageList) {
        msg.resize(strLen, '0');
    }

    LOG(INFO) << "Creating data.";
    std::vector<zmq::message_t> receiveList;
    receiveList.reserve(cnt);

    auto receiver = util::ZMQInstance::NewServer<zmq::socket_type::sub>(51200);
    ASSERT_TRUE(receiver != nullptr) << "Create instance failed";

    auto sender = util::ZMQInstance::NewClient<zmq::socket_type::pub>("127.0.0.1", 51200);
    ASSERT_TRUE(sender != nullptr) << "Create instance failed";

    auto signal = util::NewSema();
    auto timer = util::Timer();

    LOG(INFO) << "Test started";

    auto f1 = tp.submit([&]{
        util::wait_for_sema(signal);
        // Give the subscribers a chance to connect, so they don't lose any messages
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        timer.start();
        for (auto& msg: messageList) {
            auto ret = sender->send(std::move(msg));
            if (!ret) {
                return false;
            }
        }
        return true;
    });

    auto f2 = tp.submit([&]{
        util::wait_for_sema(signal);
        for (int i=0; i<cnt; i++) {
            auto ret = receiver->receive();
            if (!ret) {
                return false;
            }
            receiveList.push_back(std::move(*ret));
        }
        return true;
    });

    signal.signal(2);

    ASSERT_TRUE(f1.get()) << "Can not send msg!";
    ASSERT_TRUE(f2.get()) << "Receive invalid string!";

    LOG(INFO) << "Speed (KB/s): " << double(strLen*cnt)/timer.end()/1024;
    LOG(INFO) << "OpLen: " << strLen << ", Speed (KOp/s): " << double(cnt)/timer.end()/1000;

}
