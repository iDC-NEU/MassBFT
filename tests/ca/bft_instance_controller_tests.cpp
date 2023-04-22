//
// Created by user on 23-4-16.
//
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "ca/bft_instance_controller.h"
#include "common/timer.h"

class ControllerTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };


protected:
    constexpr static auto userName = "user";
    constexpr static auto passWord = "123456";
    constexpr static auto remoteIp = "127.0.0.1";
    inline static std::string runningPath = "/home/user/nc_bft/";
    inline static std::string jvmPath = "/home/user/.jdks/openjdk-20/bin/java ";
    inline static std::string jvmOption = "-Dlogback.configurationFile=./config/logback.xml ";
    inline static std::string classPath = "-classpath ./nc_bft.jar ";
    inline static std::string runCommand = "cd " + runningPath + "&&" + jvmPath.append(jvmOption).append(classPath) + "bftsmart.demo.neuchainplus.NeuChainServer " ;

    static bool readChannelCallback(std::string_view sv) {
        util::Timer::sleep_sec(1);
        if (sv.empty()) {
            return false;
        }
        LOG(INFO) << sv;
        return true;
    };

};

TEST_F(ControllerTest, StartTest){
    auto controller = util::BFTInstanceController::NewBFTInstanceController(remoteIp, userName, passWord);
    auto ret = controller->StartInstance(runCommand, 0);
    ASSERT_TRUE(ret);
    ret = controller->StartInstance(runCommand, 1);
    ASSERT_TRUE(ret);
    ret = controller->StartInstance(runCommand, 2);
    ASSERT_TRUE(ret);
    ret = controller->StartInstance(runCommand, 3);
    ASSERT_TRUE(ret);

    std::string out, error;
    controller->readFeedback(out, false, readChannelCallback, 0);
    controller->readFeedback(error, true, readChannelCallback, 0);
    controller->readFeedback(out, false, readChannelCallback, 1);
    controller->readFeedback(error, true, readChannelCallback, 1);
    sleep(10);
}