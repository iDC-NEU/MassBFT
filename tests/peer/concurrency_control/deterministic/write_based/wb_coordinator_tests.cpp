//
// Created by peng on 2/22/23.
//

#include "tests/coordinator_utils.h"
#include "peer/concurrency_control/deterministic/write_based/wb_coordinator.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class WBCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::Properties::LoadProperties();
    };

    void TearDown() override {
    };

};

TEST_F(WBCoordinatorTest, TestTransfer) {
    util::Properties::GetProperties()->getChaincodeProperties().install("transfer");
    constexpr int recordCount=10000;
    auto dbc = tests::CoordinatorUtils::initDB(recordCount);
    auto c = peer::cc::WBCoordinator::NewCoordinator(dbc, 10);
    tests::CoordinatorUtils::StartBenchmark([&](auto& ph) -> bool { return c->processTxnList(ph); }, recordCount);
    // validate result
    int totalValue = 0;
    for (int i=0; i < recordCount; i++) {
        std::string value;
        dbc->get(std::to_string(i), &value);
        totalValue += std::atoi(value.data());
    }
    ASSERT_TRUE(totalValue == 0);
}

TEST_F(WBCoordinatorTest, TestSessionStore) {
    util::Properties::GetProperties()->getChaincodeProperties().install("session_store");
    constexpr int recordCount=10000;
    auto dbc = tests::CoordinatorUtils::initDB(recordCount);
    auto c = peer::cc::WBCoordinator::NewCoordinator(dbc, 10);
    tests::CoordinatorUtils::StartBenchmark([&](auto& ph) -> bool { return c->processTxnList(ph); }, recordCount);
}
