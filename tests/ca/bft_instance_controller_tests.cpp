//
// Created by user on 23-4-16.
//

#include "ca/bft_instance_controller.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class ControllerTest : public ::testing::Test {
protected:
    ControllerTest() {
        sshConfig.userName = "user";
        sshConfig.password = "123456";
        sshConfig.ip = "127.0.0.1";
        sshConfig.port = -1;
    }

    void SetUp() override {
    };

    void TearDown() override {
    };

protected:
    ca::SSHConfig sshConfig;
    inline static std::string runningPath = "/home/user/nc_bft";
    inline static std::string jvmPath = "/home/user/.jdks/corretto-16.0.2/bin/java";
};

TEST_F(ControllerTest, StartTest) {
    std::vector<std::unique_ptr<ca::BFTInstanceController>> ctlList(4);
    for (int i=0; i<4; i++) {
        ctlList[i] = ca::BFTInstanceController::NewBFTInstanceController(sshConfig, 0, i, runningPath, jvmPath);
        ctlList[i]->stopAndClean();
    }
    for (int i=0; i<4; i++) {
        ASSERT_TRUE(ctlList[i]->startInstance("/home/user/nc_bft/config/hosts.config"));
    }
    for (int i=0; i<4; i++) {
        auto[success, out, err] = ctlList[i]->getChannelResponse();
        ASSERT_TRUE(success);
        LOG(INFO) << out << err;
    }
    for (int i=0; i<4; i++) {
        ctlList[i]->stopAndClean();
    }
    util::Timer::sleep_sec(5);
}

TEST_F(ControllerTest, StartDemoInstance) {
    std::vector<std::unique_ptr<ca::BFTInstanceController>> ctlList(4);
    for (int i=0; i<4; i++) {
        ctlList[i] = ca::BFTInstanceController::NewBFTInstanceController(sshConfig, 0, i, runningPath, jvmPath);
        ctlList[i]->stopAndClean();
    }
    for (int i=0; i<4; i++) {
        ASSERT_TRUE(ctlList[i]->startInstance("/home/user/nc_bft/config/hosts.config"));
    }
    for (int i=0; i<1000; i++) {    // 1000 sec
        ctlList[0]->getChannelResponse(1);
    }
    for (int i=0; i<4; i++) {
        ctlList[i]->stopAndClean();
    }
}