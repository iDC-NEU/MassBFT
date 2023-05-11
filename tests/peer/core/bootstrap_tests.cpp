//
// Created by user on 23-5-11.
//

#include "peer/core/bootstrap.h"
#include "peer/core/single_pbft_controller.h"
#include "tests/mock_property_generator.h"
#include "common/reliable_zeromq.h"
#include "common/meta_rpc_server.h"
#include "common/crypto.h"
#include "common/timer.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class BootstrapTest : public ::testing::Test {
protected:
    BootstrapTest() {
        util::OpenSSLSHA256::initCrypto();
    }

    void SetUp() override {
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    };

    void TearDown() override {
        util::Timer::sleep_sec(2);
        util::MetaRpcServer::Stop();
    };

protected:
    static auto getAndInitModules(bool distributed) {
        auto* properties = util::Properties::GetProperties();
        properties->getCustomProperties(util::Properties::JVM_PATH) = "/home/user/.jdks/corretto-16.0.2/bin/java";
        properties->getCustomProperties(util::Properties::DISTRIBUTED_SETTING) = distributed;
        auto modules = peer::core::ModuleFactory::NewModuleFactory(util::Properties::GetSharedProperties());
        CHECK(modules != nullptr);
        auto [bccsp, tp] = modules->getOrInitBCCSPAndThreadPool();
        CHECK(bccsp != nullptr && tp != nullptr);
        auto portMap = modules->getOrInitZMQPortUtilMap();
        CHECK(portMap != nullptr);
        auto contentStorage = modules->getOrInitContentStorage();
        CHECK(contentStorage != nullptr);
        auto replicator = modules->getOrInitReplicator();
        CHECK(replicator != nullptr);
        return modules;
    }
};

TEST_F(BootstrapTest, BasicTest) {
    tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
    getAndInitModules(false);
    getAndInitModules(true);
}

TEST_F(BootstrapTest, TestBFTController) {
    std::vector<std::unique_ptr<peer::core::ModuleFactory>> modList(4);
    std::vector<std::unique_ptr<::peer::core::SinglePBFTController>> bftControllerList(4);
    for (int i=0; i<4; i++) {
        tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
        tests::MockPropertyGenerator::SetLocalId(0, i);
        modList[i] = getAndInitModules(false);
        bftControllerList[i] = modList[i]->newReplicatorBFTController(0);
        CHECK(bftControllerList[i] != nullptr);
    }
    for (int i=0; i<4; i++) {
        bftControllerList[i]->startInstance();
    }
    for (int i=0; i<4; i++) {
        bftControllerList[i]->waitUntilReady();
    }
}
