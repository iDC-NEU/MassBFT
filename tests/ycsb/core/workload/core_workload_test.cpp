//
// Created by peng on 11/7/22.
//

#include "gtest/gtest.h"
#include "ycsb/core/workload/core_workload.h"
#include "yaml-cpp/yaml.h"

class CoreWorkloadTest : public ::testing::Test {
protected:
    void SetUp() override {
        using namespace ycsb::core::workload;
        ycsb::utils::YCSBProperties::Proportion p{};
        p.readProportion = 0.20;
        p.updateProportion = 0.20;
        p.insertProportion = 0.20;
        p.readModifyWriteProportion = 0.20;
        p.scanProportion = 0.20;
        generator = CoreWorkload::createOperationGenerator(p);
    };

    void TearDown() override {
    };

    auto TestOperationGenerator() {
        std::vector<int> counts(5);
        for (int i = 0; i < 10000; ++i) {
            switch (generator->nextValue()) {
                case ycsb::core::Operation::READ:
                    ++counts[0];
                    break;
                case ycsb::core::Operation::UPDATE:
                    ++counts[1];
                    break;
                case ycsb::core::Operation::INSERT:
                    ++counts[2];
                    break;
                case ycsb::core::Operation::SCAN:
                    ++counts[3];
                    break;
                default:
                    ++counts[4];
            }
        }
        return counts;
    }

protected:
    ycsb::core::workload::CoreWorkload coreWorkload;
    std::unique_ptr<ycsb::core::DiscreteGenerator> generator;
};

TEST_F(CoreWorkloadTest, TestOperationGenerator) {
    auto counts = TestOperationGenerator();
    for (int i : counts) {
        EXPECT_TRUE(i < (double)10000/5*1.05) << "distribution test failed!";
        EXPECT_TRUE(i > (double)10000/5*0.95) << "distribution test failed!";
    }
}

TEST_F(CoreWorkloadTest, TestDefaultOperationGenerator) {
    ycsb::utils::YCSBProperties::Proportion p{};
    p.readProportion = 0.95;
    p.updateProportion = 0.05;
    this->generator = ycsb::core::workload::CoreWorkload::createOperationGenerator(p);
    auto counts = TestOperationGenerator();
    EXPECT_TRUE(counts[0] < (double)10000*0.95*1.1) << "distribution test failed!";
    EXPECT_TRUE(counts[0] > (double)10000*0.95*0.9) << "distribution test failed!";
    EXPECT_TRUE(counts[1] < (double)10000*0.05*1.1) << "distribution test failed!";
    EXPECT_TRUE(counts[1] > (double)10000*0.05*0.9) << "distribution test failed!";
}

TEST_F(CoreWorkloadTest, TestBuildKey) {
    EXPECT_TRUE(ycsb::core::workload::CoreWorkload::buildKeyName(1000, 1, true) == "user1000");
    EXPECT_TRUE(ycsb::core::workload::CoreWorkload::buildKeyName(1000, 1, false) == "user5952875239596136740");
    EXPECT_TRUE(ycsb::core::workload::CoreWorkload::buildKeyName(1000, 25, true) == "user0000000000000000000001000");
    EXPECT_TRUE(ycsb::core::workload::CoreWorkload::buildKeyName(1000, 25, false) == "user0000005952875239596136740");
}

TEST_F(CoreWorkloadTest, TestGetFieldLengthGenerator1) {
    using namespace ycsb::core::workload;
    auto* property = util::Properties::GetProperties();
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::FIELD_LENGTH_DISTRIBUTION_PROPERTY, "constant");
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::FIELD_LENGTH_PROPERTY, 35);
    auto p = ycsb::utils::YCSBProperties::NewFromProperty(*property);
    auto gen = CoreWorkload::getFieldLengthGenerator(*p);
    const auto val = gen->nextValue();
    EXPECT_TRUE(val == 35);
    for (int i=0; i<1000; i++) {
        EXPECT_TRUE(val == gen->nextValue());
    }
}

TEST_F(CoreWorkloadTest, TestGetFieldLengthGenerator2) {
    YAML::Node tmp;
    using namespace ycsb::core::workload;
    auto* property = util::Properties::GetProperties();
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::FIELD_LENGTH_DISTRIBUTION_PROPERTY, "uniform");
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::FIELD_LENGTH_PROPERTY, 50);
    ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::MIN_FIELD_LENGTH_PROPERTY, 41);
    auto p = ycsb::utils::YCSBProperties::NewFromProperty(*property);
    auto gen = CoreWorkload::getFieldLengthGenerator(*p);
    std::unordered_map<uint64_t, int> counts;
    for (int i=0; i<100000; i++) {
        auto val = gen->nextValue();
        EXPECT_TRUE(val >= 41);
        EXPECT_TRUE(val <= 50);
        counts[val]++;
    }
    for (const auto& pair :counts) {
        EXPECT_TRUE(pair.second >= 100000.0/10*0.95);
        EXPECT_TRUE(pair.second <= 100000.0/10*1.05);
    }
}

// TODO: test buildSingleValue