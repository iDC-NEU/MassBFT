//
// Created by peng on 11/6/22.
//

#include "client/ycsb/ycsb_engine.h"
#include "tests/mock_property_generator.h"
#include "tests/peer/mock_peer.h"
#include "gtest/gtest.h"

class YCSBTest : public ::testing::Test {
protected:
    void SetUp() override {
        tests::MockPropertyGenerator::GenerateDefaultProperties(4, 4);
        tests::MockPropertyGenerator::SetLocalId(2, 2);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::THREAD_COUNT_PROPERTY, 1);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::OPERATION_COUNT_PROPERTY, 10000);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 1000);

        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 100);
    };

    void TearDown() override {

    };

    static void SetupDefaultSingleWorkloadParam() {
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::RECORD_COUNT_PROPERTY, 100);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::OPERATION_COUNT_PROPERTY, 500);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 100);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::THREAD_COUNT_PROPERTY, 1);
        util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 50);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::FIELD_COUNT_PROPERTY, 10);

        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 0.00);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::UPDATE_PROPORTION_PROPERTY, 0.00);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::INSERT_PROPORTION_PROPERTY, 0.00);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::SCAN_PROPORTION_PROPERTY, 0.00);
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READMODIFYWRITE_PROPORTION_PROPERTY, 0.00);
    }

protected:
    size_t checkSize = 1;   // 10 for default cc, 1 for row level ycsb
};

TEST_F(YCSBTest, BasicTest) {
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, false, false);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, TwoWorkerTest) {
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::THREAD_COUNT_PROPERTY, 2);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, false, false);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, OneWorkerPerformanceTest) {
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::OPERATION_COUNT_PROPERTY, 100000);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 10000);
    util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 1000);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, false, false);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, OverloadTest) {
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::OPERATION_COUNT_PROPERTY, 10000000);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::TARGET_THROUGHPUT_PROPERTY, 300000);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::THREAD_COUNT_PROPERTY, 10);
    util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 5000);
    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, false);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
}

TEST_F(YCSBTest, ReadWorkloadTest) {
    SetupDefaultSingleWorkloadParam();
    // set read proportion
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);

    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, true);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
    const auto& result = peer.getExecutionResult();
    ASSERT_TRUE(!result.empty());
    for (const auto& it: result) {
        auto& user = get<0>(it);
        ASSERT_TRUE(user->getCCNameSV() == "ycsb");
        ASSERT_TRUE(user->getFuncNameSV() == "r");
        auto& reads = get<1>(it);
        auto& writes = get<2>(it);
        ASSERT_TRUE(reads->size() == checkSize);
        ASSERT_TRUE(writes->empty());
        ASSERT_TRUE(!reads->at(checkSize - 1)->getKeySV().empty());
        ASSERT_TRUE(!reads->at(checkSize - 1)->getValueSV().empty());
    }
}

TEST_F(YCSBTest, ReadOneFieldTest) {
    SetupDefaultSingleWorkloadParam();
    // set read proportion
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_ALL_FIELDS_PROPERTY, false);

    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, true);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
    const auto& result = peer.getExecutionResult();
    ASSERT_TRUE(!result.empty());
    for (const auto& it: result) {
        auto& user = get<0>(it);
        ASSERT_TRUE(user->getCCNameSV() == "ycsb");
        ASSERT_TRUE(user->getFuncNameSV() == "r");
        auto& reads = get<1>(it);
        auto& writes = get<2>(it);
        ASSERT_TRUE(reads->size() == 1);
        ASSERT_TRUE(writes->empty());
        ASSERT_TRUE(!reads->at(0)->getKeySV().empty());
        ASSERT_TRUE(!reads->at(0)->getValueSV().empty());
    }
}

TEST_F(YCSBTest, InsertWorkloadTest) {
    SetupDefaultSingleWorkloadParam();
    // set insert proportion
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::INSERT_PROPORTION_PROPERTY, 1.00);

    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, true);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
    const auto& result = peer.getExecutionResult();
    ASSERT_TRUE(!result.empty());
    for (const auto& it: result) {
        auto& user = get<0>(it);
        ASSERT_TRUE(user->getCCNameSV() == "ycsb");
        ASSERT_TRUE(user->getFuncNameSV() == "i");
        auto& reads = get<1>(it);
        auto& writes = get<2>(it);
        ASSERT_TRUE(reads->empty());
        ASSERT_TRUE(writes->size() == checkSize);
        ASSERT_TRUE(!writes->at(checkSize - 1)->getKeySV().empty());
        ASSERT_TRUE(!writes->at(checkSize - 1)->getValueSV().empty());
    }
}

TEST_F(YCSBTest, RMWWorkloadTest) {
    SetupDefaultSingleWorkloadParam();
    // set insert proportion
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READMODIFYWRITE_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_ALL_FIELDS_PROPERTY, false);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::WRITE_ALL_FIELDS_PROPERTY, false);

    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, true);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
    const auto& result = peer.getExecutionResult();
    ASSERT_TRUE(!result.empty());
    for (const auto& it: result) {
        auto& user = get<0>(it);
        ASSERT_TRUE(user->getCCNameSV() == "ycsb");
        ASSERT_TRUE(user->getFuncNameSV() == "m");
        auto& reads = get<1>(it);
        auto& writes = get<2>(it);
        ASSERT_TRUE(reads->size() == 1);
        ASSERT_TRUE(writes->size() == 1);
        ASSERT_TRUE(!reads->at(0)->getKeySV().empty());
        ASSERT_TRUE(!reads->at(0)->getValueSV().empty());
        ASSERT_TRUE(!writes->at(0)->getKeySV().empty());
        ASSERT_TRUE(!writes->at(0)->getValueSV().empty());
    }
}

TEST_F(YCSBTest, RMWWorkloadTest2) {
    SetupDefaultSingleWorkloadParam();
    // set insert proportion
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READMODIFYWRITE_PROPORTION_PROPERTY, 1.00);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::READ_ALL_FIELDS_PROPERTY, true);
    client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::WRITE_ALL_FIELDS_PROPERTY, true);

    auto* p = util::Properties::GetProperties();
    tests::peer::Peer peer(*p, true, true);
    client::ycsb::YCSBEngine engine(*p);
    engine.startTest();
    const auto& result = peer.getExecutionResult();
    ASSERT_TRUE(!result.empty());
    for (const auto& it: result) {
        auto& user = get<0>(it);
        ASSERT_TRUE(user->getCCNameSV() == "ycsb");
        ASSERT_TRUE(user->getFuncNameSV() == "m");
        auto& reads = get<1>(it);
        auto& writes = get<2>(it);
        ASSERT_TRUE(reads->size() == checkSize);
        ASSERT_TRUE(writes->size() == checkSize);
        ASSERT_TRUE(!reads->at(checkSize - 1)->getKeySV().empty());
        ASSERT_TRUE(!reads->at(checkSize - 1)->getValueSV().empty());
        ASSERT_TRUE(!writes->at(checkSize - 1)->getKeySV().empty());
        ASSERT_TRUE(!writes->at(checkSize - 1)->getValueSV().empty());
    }
}
