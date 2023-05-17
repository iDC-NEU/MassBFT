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

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "proto/user_request.h"

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

    // must NOT return nullptr
    static auto CreateSignedEnvelop(const std::string& ski, const std::shared_ptr<::util::BCCSP>& bccsp) {
        std::unique_ptr<proto::Envelop> envelop(new proto::Envelop());
        proto::SignatureString sig = { ski, std::make_shared<std::string>() };
        auto key = bccsp->GetKey(ski);
        CHECK(key->Private());
        std::string payload("payload for an envelop" + std::to_string(rand()));
        auto ret = key->Sign(payload.data(), payload.size());
        CHECK(ret != std::nullopt);
        sig.digest = *ret;
        envelop->setPayload(std::move(payload));
        envelop->setSignature(std::move(sig));
        return envelop;
    }

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
    std::vector<std::unique_ptr<util::ZMQInstance>> clientList;
    for (int i=0; i<groupCount; i++) {
        auto portMap = mcList[i*nodeCountPerGroup]->getModuleFactory().getOrInitZMQPortUtilMap();
        auto& localPortMap = portMap->at(i).at(0);
        auto clientPort = localPortMap->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[0];
        auto client = util::ZMQInstance::NewClient<zmq::socket_type::push>("127.0.0.1", clientPort);
        CHECK(client != nullptr);
        clientList.push_back(std::move(client));
    }
    util::Timer::sleep_sec(1);
    auto [bccsp, tp] = mcList[0]->getModuleFactory().getOrInitBCCSPAndThreadPool();
    for (int i=0; i<200 * 100; i++) {
        auto envelop = CreateSignedEnvelop("0_0", bccsp);
        std::string buf;
        CHECK(envelop->serializeToString(&buf));
        clientList[i%clientList.size()]->send(std::move(buf));
    }
    util::Timer::sleep_sec(10);
}