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
#include "peer/chaincode/orm.h"
#include "peer/chaincode/chaincode.h"

class ModuleCoordinatorTest : public ::testing::Test {
protected:
    ModuleCoordinatorTest() {
        util::OpenSSLSHA256::initCrypto();
        tests::MockPropertyGenerator::GenerateDefaultProperties(groupCount, nodeCountPerGroup);
        util::Properties::SetProperties(util::Properties::REPLICATOR_LOWEST_PORT, 13000);
    }

    void SetUp() override {
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
        // avoid database overload
        util::Properties::SetProperties(util::Properties::ARIA_WORKER_COUNT, 1);
        // init ycsb config
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::RECORD_COUNT_PROPERTY, 10000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 30000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 1000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 1);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::READ_PROPORTION_PROPERTY, 0.50);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::UPDATE_PROPORTION_PROPERTY, 0.50);
        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 100);
        util::Properties::SetProperties(util::Properties::BATCH_TIMEOUT_MS, 1000);
        // load ycsb database
    };

    void TearDown() override {
        util::MetaRpcServer::Stop();
    };

    static bool FillCCData(::peer::db::DBConnection& db, const auto& dbName) {
        auto orm = ::peer::chaincode::ORM::NewORMFromDBInterface(&db);
        auto cc = ::peer::chaincode::NewChaincodeByName(dbName, std::move(orm));
        if (cc->InitDatabase() != 0) {
            return false;
        }
        auto [reads, writes] = cc->reset();
        return db.syncWriteBatch([&](auto* batch) ->bool {
            for (const auto& it: *writes) {
                batch->Put({it->getKeySV().data(), it->getKeySV().size()}, {it->getValueSV().data(), it->getValueSV().size()});
            }
            return true;
        });
    }

protected:
    static void InitNodeConfig(int groupId, int nodeId) {
        tests::MockPropertyGenerator::SetLocalId(groupId, nodeId);
        util::Properties::SetProperties(util::Properties::DISTRIBUTED_SETTING, false);
    }

    const int nodeCountPerGroup = 3;
    const int groupCount = 1;
};

TEST_F(ModuleCoordinatorTest, BasicTest2_4) {
    std::vector<std::unique_ptr<peer::core::ModuleCoordinator>> mcList;
    for (int i=0; i<groupCount; i++) {
        for (int j=0; j<nodeCountPerGroup; j++) {
            InitNodeConfig(i, j);
            auto mc = peer::core::ModuleCoordinator::NewModuleCoordinator(util::Properties::GetSharedProperties());
            CHECK(mc != nullptr);
            CHECK(FillCCData(*mc->getDBHandle(), "ycsb"));
            mcList.push_back(std::move(mc));
        }
    }
    for (auto& it: mcList) {
        CHECK(it->startInstance());
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
    util::Timer::sleep_sec(3);
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