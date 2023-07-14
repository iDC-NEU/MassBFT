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
    std::vector<ca::NodeHostConfig> hostList;
    for (int i=0; i<4; i++) {
        ca::NodeHostConfig host;
        host.processId = i;
        host.ip = "127.0.0.1";
        host.serverToServerPort = 11000 + i*10;
        host.serverToClientPort = 11001 + i*10;
        host.rpcPort =  51204 + i*5;
        hostList.push_back(host);
    }
    std::vector<std::unique_ptr<ca::BFTInstanceController>> ctlList(4);
    for (int i=0; i<4; i++) {
        ctlList[i] = ca::BFTInstanceController::NewBFTInstanceController(sshConfig, 0, i, runningPath, jvmPath);
        ctlList[i]->prepareConfigurationFile(hostList);
        ctlList[i]->stopAndClean();
    }
    for (int i=0; i<4; i++) {
        ASSERT_TRUE(ctlList[i]->startInstance(""));
    }
    for (int i=0; i<4; i++) {
        auto success = ctlList[i]->isInstanceReady(20 * 1000);
        ASSERT_TRUE(success && *success);
    }
    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (int i=0; i<4; i++) {
        ctlList[i]->stopAndClean();
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

TEST_F(ControllerTest, StartDemoInstance) {
    std::vector<ca::NodeHostConfig> hostList;
    for (int i=0; i<4; i++) {
        ca::NodeHostConfig host;
        host.processId = i;
        host.ip = "127.0.0.1";
        host.serverToServerPort = 11000 + i*10;
        host.serverToClientPort = 11001 + i*10;
        host.rpcPort =  51204 + i*5;
        hostList.push_back(host);
    }
    std::vector<std::unique_ptr<ca::BFTInstanceController>> ctlList(4);
    for (int i=0; i<4; i++) {
        ctlList[i] = ca::BFTInstanceController::NewBFTInstanceController(sshConfig, 0, i, runningPath, jvmPath);
        ctlList[i]->prepareConfigurationFile(hostList);
        ctlList[i]->stopAndClean();
    }
    for (int i=0; i<4; i++) {
        ASSERT_TRUE(ctlList[i]->startInstance(""));
    }
    auto success = ctlList[0]->isInstanceReady(20 * 1000);
    ASSERT_TRUE(success && *success);
    std::this_thread::sleep_for(std::chrono::seconds(10));
    for (int i=0; i<4; i++) {
        ctlList[i]->stopAndClean();
    }
}