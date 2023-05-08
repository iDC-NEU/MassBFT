//
// Created by peng on 2/22/23.
//

#include "tests/coordinator_utils.h"
#include "peer/concurrency_control/deterministic/coordinator_impl.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class CoordinatorImplTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::Properties::LoadProperties();
    };

    void TearDown() override {
    };

};


TEST_F(CoordinatorImplTest, TestTransfer) {
    util::Properties::GetProperties()->getChaincodeProperties().install("transfer");
    constexpr int recordCount=10000;
    auto dbc = tests::CoordinatorUtils::initDB(recordCount);
    auto c = peer::cc::CoordinatorImpl::NewCoordinator(dbc, 10);
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

TEST_F(CoordinatorImplTest, TestSessionStore) {
    util::Properties::GetProperties()->getChaincodeProperties().install("session_store");
    constexpr int recordCount=10000;
    auto dbc = tests::CoordinatorUtils::initDB(recordCount);
    auto c = peer::cc::CoordinatorImpl::NewCoordinator(dbc, 10);
    tests::CoordinatorUtils::StartBenchmark([&](auto& ph) -> bool { return c->processTxnList(ph); }, recordCount);
}