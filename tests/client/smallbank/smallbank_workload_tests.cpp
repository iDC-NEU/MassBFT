//
// Created by user on 23-8-13.
//

#include "client/core/default_engine.h"
#include "client/core/write_through_db.h"
#include "client/small_bank/small_bank_property.h"
#include "client/small_bank/small_bank_engine.h"
#include "peer/chaincode/small_bank_chaincode.h"

#include "tests/mock_property_generator.h"
#include "gtest/gtest.h"

using namespace client::small_bank;

class MockStatus: public client::core::DBStatus {
public:
    std::unique_ptr<::proto::Block> getBlock(int) override { return nullptr; }

    bool connect(int, int) override  { return true; }

    bool getTop(int& blockNumber, int, int) override  {
        blockNumber = -1;
        return true;
    }
};

class MockSmallBankDBFactory : public client::core::DBFactory {
public:
    explicit MockSmallBankDBFactory(const util::Properties &n)
            : client::core::DBFactory(n) {
        db = peer::db::DBConnection::NewConnection("ChaincodeTestDB");
        CHECK(db != nullptr) << "failed to init db!";
        auto orm = peer::chaincode::ORM::NewORMFromDBInterface(db);
        chaincode = peer::chaincode::NewChaincodeByName("sb", std::move(orm));
        CHECK(chaincode != nullptr) << "failed to init chaincode!";
    }

    [[nodiscard]] std::unique_ptr<client::core::DB> newDB() const override {
        initDatabase();
        return std::make_unique<client::core::WriteThroughDB>(chaincode.get());
    }

    [[nodiscard]] std::unique_ptr<client::core::DBStatus> newDBStatus() const override {
        return std::make_unique<MockStatus>();
    }

    void initDatabase() const {
        CHECK(chaincode->InitDatabase() == 0);
        ::proto::KVList reads, writes;
        chaincode->reset(reads, writes);
        CHECK(db->syncWriteBatch([&](auto* batch) ->bool {
            for (const auto& it: writes) {
                batch->Put({it->getKeySV().data(), it->getKeySV().size()}, {it->getValueSV().data(), it->getValueSV().size()});
            }
            return true;
        }));
    }

    std::shared_ptr<peer::db::DBConnection> db;
    std::unique_ptr<peer::chaincode::Chaincode> chaincode;
};

struct MockSmallBankFactory {
    static std::shared_ptr<client::core::Workload> CreateWorkload(const util::Properties &) {
        return std::make_shared<SmallBankWorkload>();
    }

    static std::unique_ptr<SmallBankProperties> CreateProperty(const util::Properties &n) {
        return SmallBankProperties::NewFromProperty(n);
    }

    static std::unique_ptr<client::core::DBFactory> CreateDBFactory(const util::Properties &n) {
        mockDBFactory = new MockSmallBankDBFactory(n);
        return std::unique_ptr<client::core::DBFactory>(mockDBFactory);
    }

    inline static MockSmallBankDBFactory* mockDBFactory = nullptr;
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