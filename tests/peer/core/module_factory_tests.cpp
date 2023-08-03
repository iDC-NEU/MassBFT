//
// Created by user on 23-5-11.
//

#include "peer/core/module_factory.h"
#include "peer/core/single_pbft_controller.h"
#include "peer/consensus/block_order/global_ordering.h"
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
        util::MetaRpcServer::Stop();
    };

protected:
    static auto GetAndInitModules(bool distributed) {
        util::Properties::SetProperties(util::Properties::DISTRIBUTED_SETTING, distributed);
        auto properties = util::Properties::GetSharedProperties();
        auto modules = peer::core::ModuleFactory::NewModuleFactory(properties);
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
    tests::MockPropertyGenerator::SetLocalId(0, 1);
    GetAndInitModules(false);
    GetAndInitModules(true);
    util::Timer::sleep_sec(2);
}

TEST_F(BootstrapTest, TestBFTController) {
    std::vector<std::unique_ptr<peer::core::ModuleFactory>> modList(4);
    std::vector<std::unique_ptr<::peer::core::SinglePBFTController>> bftControllerList(4);
    for (int i=0; i<4; i++) {
        tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
        tests::MockPropertyGenerator::SetLocalId(0, i);
        modList[i] = GetAndInitModules(false);
        CHECK(modList[i]->initUserRPCController());
        // groupId is set to 1 to coverage more code
        bftControllerList[i] = modList[i]->newReplicatorBFTController(1);
        CHECK(bftControllerList[i] != nullptr);
    }
    for (int i=0; i<4; i++) {
        bftControllerList[i]->startInstance();
    }
    for (int i=0; i<4; i++) {
        bftControllerList[i]->waitUntilReady();
    }
    util::Timer::sleep_sec(2);
}

TEST_F(BootstrapTest, TestGlobalOrdering) {
    tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
    tests::MockPropertyGenerator::SetLocalId(0, 1);
    auto modules = GetAndInitModules(false);
    auto orderCAB = std::make_shared<peer::consensus::v2::OrderACB>([](int, int) ->bool {
        return true;
    });
    auto ret = modules->newGlobalBlockOrdering(orderCAB);
    CHECK(ret != nullptr);
    util::Timer::sleep_sec(2);
}
