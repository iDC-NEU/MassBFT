//
// Created by user on 23-5-16.
//

#include "peer/core/module_coordinator.h"

#include "common/crypto.h"
#include "common/timer.h"
#include "common/reliable_zeromq.h"
#include "common/property.h"
#include "tests/mock_property_generator.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class ModuleCoordinatorTest : public ::testing::Test {
protected:
    ModuleCoordinatorTest() {
        util::OpenSSLSHA256::initCrypto();
        tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
    }

    void SetUp() override {
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

protected:
    static void InitNodeConfig4_4(int groupId, int nodeId) {
        tests::MockPropertyGenerator::SetLocalId(groupId, nodeId);
        auto* properties = util::Properties::GetProperties();
        properties->getCustomProperties(util::Properties::JVM_PATH) = "/home/user/.jdks/corretto-16.0.2/bin/java";
        properties->getCustomProperties(util::Properties::DISTRIBUTED_SETTING) = false;
    }
};

TEST_F(ModuleCoordinatorTest, BasicTest) {
    std::vector<std::unique_ptr<peer::core::ModuleCoordinator>> mcList;
    for (int i=0; i<4; i++) {
        for (int j=0; j<4; j++) {
            InitNodeConfig4_4(i, j);
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
    sleep(5);
}