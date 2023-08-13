//
// Created by user on 23-8-10.
//

#include "client/core/default_engine.h"
#include "client/core/write_through_db.h"
#include "client/tpcc/tpcc_workload.h"
#include "client/tpcc/tpcc_property.h"
#include "peer/chaincode/tpcc_chaincode.h"

#include "tests/mock_property_generator.h"
#include "gtest/gtest.h"

using namespace client::tpcc;

class MockStatus: public client::core::DBStatus {
public:
    std::unique_ptr<::proto::Block> getBlock(int) override { return nullptr; }

    bool connect(int, int) override  { return true; }

    bool getTop(int& blockNumber, int, int) override  {
        blockNumber = -1;
        return true;
    }
};

class MockTPCCDBFactory : public client::core::DBFactory {
public:
    explicit MockTPCCDBFactory(const util::Properties &n)
        : client::core::DBFactory(n) {
        db = peer::db::DBConnection::NewConnection("ChaincodeTestDB");
        CHECK(db != nullptr) << "failed to init db!";
        auto orm = peer::chaincode::ORM::NewORMFromDBInterface(db);
        chaincode = peer::chaincode::NewChaincodeByName("tpcc", std::move(orm));
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

struct MockTPCCFactory {
    static std::shared_ptr<client::core::Workload> CreateWorkload(const util::Properties &) {
        return std::make_shared<TPCCWorkload>();
    }

    static std::unique_ptr<TPCCProperties> CreateProperty(const util::Properties &n) {
        return TPCCProperties::NewFromProperty(n);
    }

    static std::unique_ptr<client::core::DBFactory> CreateDBFactory(const util::Properties &n) {
        mockDBFactory = new MockTPCCDBFactory(n);
        return std::unique_ptr<client::core::DBFactory>(mockDBFactory);
    }

    inline static MockTPCCDBFactory* mockDBFactory = nullptr;
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