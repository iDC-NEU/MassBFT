//
// Created by user on 23-5-16.
//

#include "peer/core/module_coordinator.h"
#include "peer/core/bootstrap.h"

#include "common/bccsp.h"
#include "common/crypto.h"
#include "common/timer.h"
#include "common/reliable_zeromq.h"
#include "common/property.h"

#include "tests/mock_property_generator.h"

#include "ycsb/engine.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class ModuleCoordinatorTest : public ::testing::Test {
protected:
    ModuleCoordinatorTest() {
        util::OpenSSLSHA256::initCrypto();
        tests::MockPropertyGenerator::GenerateDefaultProperties(groupCount, nodeCountPerGroup);
    }

    void SetUp() override {
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

protected:
    static void InitNodeConfig(int groupId, int nodeId) {
        tests::MockPropertyGenerator::SetLocalId(groupId, nodeId);
        auto* properties = util::Properties::GetProperties();
        properties->getCustomProperties(util::Properties::JVM_PATH) = "/home/user/.jdks/corretto-16.0.2/bin/java";
        properties->getCustomProperties(util::Properties::DISTRIBUTED_SETTING) = false;
    }

    const int nodeCountPerGroup = 4;
    const int groupCount = 4;
};

TEST_F(ModuleCoordinatorTest, BasicTest2_4) {
    std::vector<std::unique_ptr<peer::core::ModuleCoordinator>> mcList;
    for (int i=0; i<groupCount; i++) {
        for (int j=0; j<nodeCountPerGroup; j++) {
            InitNodeConfig(i, j);
            auto mc = peer::core::ModuleCoordinator::NewModuleCoordinator(util::Properties::GetSharedProperties());
            CHECK(mc != nullptr);
            mcList.push_back(std::move(mc));
        }
    }
    for (auto& it: mcList) {
        it->startInstance();
    }
    for (auto& it: mcList) {
        it->waitInstanceReady();
    }
    // for the leaders
    std::vector<std::unique_ptr<ycsb::YCSBEngine>> clientList;
    for (int i=0; i<groupCount; i++) {
        tests::MockPropertyGenerator::SetLocalId(i, 0);
        auto* p = util::Properties::GetProperties();
        auto engine = std::make_unique<ycsb::YCSBEngine>(*p);
        clientList.push_back(std::move(engine));
    }
    util::Timer::sleep_sec(1);
    LOG(INFO) << "Test start!";
    for (auto& it: clientList) {
        it->startTestNoWait();
    }
    for (auto& it: clientList) {
        it->waitUntilFinish();
    }
    LOG(INFO) << "Send finished!";
    util::Timer::sleep_sec(10);
}