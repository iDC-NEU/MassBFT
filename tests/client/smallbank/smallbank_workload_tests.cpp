//
// Created by user on 23-8-13.
//

#include "tests/client/client_utils.h"
#include "client/small_bank/small_bank_property.h"
#include "client/small_bank/small_bank_engine.h"

#include "gtest/gtest.h"

using namespace client::small_bank;

struct MockSmallBankFactory {
    static std::shared_ptr<client::core::Workload> CreateWorkload(const util::Properties &) {
        return std::make_shared<SmallBankWorkload>();
    }

    static std::unique_ptr<SmallBankProperties> CreateProperty(const util::Properties &n) {
        return SmallBankProperties::NewFromProperty(n);
    }

    static std::unique_ptr<client::core::DBFactory> CreateDBFactory(const util::Properties &n) {
        mockDBFactory = new tests::MockDBFactory(n, "sb");
        return std::unique_ptr<client::core::DBFactory>(mockDBFactory);
    }

    inline static tests::MockDBFactory* mockDBFactory = nullptr;
};

using MockSmallBankEngine = client::core::DefaultEngine<MockSmallBankFactory, SmallBankProperties>;

class SmallBankWorkloadTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
        tests::MockPropertyGenerator::GenerateDefaultProperties(1, 3);
        tests::MockPropertyGenerator::SetLocalId(0, 0);

        SmallBankProperties::SetProperties(SmallBankProperties::ACCOUNTS_COUNT_PROPERTY, 100000);
        SmallBankProperties::SetProperties(SmallBankProperties::PROB_ACCOUNT_HOTSPOT, 0.3);
        SmallBankProperties::SetProperties(SmallBankProperties::THREAD_COUNT_PROPERTY, 1);
    };

    void TearDown() override { };
};

TEST_F(SmallBankWorkloadTest, BalancePropotionTest) {
    SmallBankProperties::SetProperties(SmallBankProperties::BALANCE_PROPORTION, 1);

    auto* p = util::Properties::GetProperties();
    MockSmallBankEngine engine(*p);
    engine.startTest();
}

TEST_F(SmallBankWorkloadTest, DepositCheckingTest) {
    SmallBankProperties::SetProperties(SmallBankProperties::DEPOSIT_CHECKING_PROPORTION, 1);

    auto* p = util::Properties::GetProperties();
    MockSmallBankEngine engine(*p);
    engine.startTest();
}

TEST_F(SmallBankWorkloadTest, TransactSavingTest) {
    SmallBankProperties::SetProperties(SmallBankProperties::TRANSACT_SAVING_PROPORTION, 1);

    auto* p = util::Properties::GetProperties();
    MockSmallBankEngine engine(*p);
    engine.startTest();
}

TEST_F(SmallBankWorkloadTest, AmalgamateTest) {
    SmallBankProperties::SetProperties(SmallBankProperties::AMALGAMATE_PROPORTION, 1);

    auto* p = util::Properties::GetProperties();
    MockSmallBankEngine engine(*p);
    engine.startTest();
}

TEST_F(SmallBankWorkloadTest, WriteCheckTest) {
    SmallBankProperties::SetProperties(SmallBankProperties::WRITE_CHECK_PROPORTION, 1);

    auto* p = util::Properties::GetProperties();
    MockSmallBankEngine engine(*p);
    engine.startTest();
}