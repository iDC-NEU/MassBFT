//
// Created by user on 23-8-10.
//

#include "tests/client/client_utils.h"
#include "client/tpcc/tpcc_workload.h"
#include "client/tpcc/tpcc_property.h"

#include "gtest/gtest.h"

using namespace client::tpcc;

struct MockTPCCFactory {
    static std::shared_ptr<client::core::Workload> CreateWorkload(const util::Properties &) {
        return std::make_shared<TPCCWorkload>();
    }

    static std::unique_ptr<TPCCProperties> CreateProperty(const util::Properties &n) {
        return TPCCProperties::NewFromProperty(n);
    }

    static std::unique_ptr<client::core::DBFactory> CreateDBFactory(const util::Properties &n) {
        mockDBFactory = new tests::MockDBFactory(n, "tpcc");
        return std::unique_ptr<client::core::DBFactory>(mockDBFactory);
    }

    inline static tests::MockDBFactory* mockDBFactory = nullptr;
};

using MockTPCCEngine = client::core::DefaultEngine<MockTPCCFactory, TPCCProperties>;

class TPCCWorkloadTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
        tests::MockPropertyGenerator::GenerateDefaultProperties(1, 3);
        tests::MockPropertyGenerator::SetLocalId(0, 0);

        TPCCProperties::SetProperties(TPCCProperties::USE_RANDOM_SEED, false);
        TPCCProperties::SetProperties(TPCCProperties::THREAD_COUNT_PROPERTY, 1);
    };

    void TearDown() override { };
};

TEST_F(TPCCWorkloadTest, NewOrderTest) {
    TPCCProperties::SetProperties(TPCCProperties::NEW_ORDER_PROPORTION_PROPERTY, 1.0);

    auto* p = util::Properties::GetProperties();
    MockTPCCEngine engine(*p);
    engine.startTest();
}

TEST_F(TPCCWorkloadTest, PaymentTest) {
    TPCCProperties::SetProperties(TPCCProperties::PAYMENT_PROPORTION_PROPERTY, 1.0);

    auto* p = util::Properties::GetProperties();
    MockTPCCEngine engine(*p);
    engine.startTest();
}