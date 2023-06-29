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