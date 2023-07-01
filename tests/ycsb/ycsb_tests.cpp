//
// Created by peng on 11/6/22.
//

#include "ycsb/engine.h"
#include "tests/mock_property_generator.h"
#include "tests/peer/mock_peer.h"
#include "gtest/gtest.h"

class YCSBTest : public ::testing::Test {
protected:
    void SetUp() override {
        tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
        tests::MockPropertyGenerator::SetLocalId(2, 2);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 1);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 10000);
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 1000);

        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 100);
    };

    void TearDown() override {

    };

};

TEST_F(YCSBTest, BasicTest) {
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p);
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, TwoWorkerTest) {
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 2);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p);
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, OneWorkerPerformanceTest) {
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 100000);
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 10000);
    util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 1000);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p);
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, OverloadTest) {
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::OPERATION_COUNT_PROPERTY, 10000000);
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 300000);
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::THREAD_COUNT_PROPERTY, 10);
    util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 5000);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true);
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, WorkloadDefualtProportionTest) {
    // r:0.95
    // u:0.05
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true);
    ycsb::YCSBEngine engine(*p);
    engine.startTest();
    auto count = peer.getOpCount();
    ASSERT_TRUE(!count.empty());
    ASSERT_TRUE(count["r"] < (double)10000*0.95*1.1);
    ASSERT_TRUE(count["r"] > (double)10000*0.95*0.9);
    ASSERT_TRUE(count["u"] < (double)10000*0.05*1.1);
    ASSERT_TRUE(count["u"] > (double)10000*0.05*0.9);
}