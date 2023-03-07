//
// Created by peng on 2/21/23.
//

#include "peer/chaincode/simple_transfer.h"
#include "peer/db/rocksdb_connection.h"

#include "gtest/gtest.h"

class ChaincodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        initDB();
    };

    void TearDown() override {
        chaincode.reset();
        db.reset();
    };

    static std::string ParamToString(const std::vector<std::string>& args) {
        std::string argRaw;
        zpp::bits::out out(argRaw);
        CHECK(!failure(out(args)));
        return argRaw;
    }

    void initDB() {
        db = peer::db::RocksdbConnection::NewConnection("ChaincodeTestDB");
        CHECK(db != nullptr) << "failed to init db!";
        auto orm = peer::chaincode::ORM::NewORMFromLeveldb(db.get());
        chaincode = peer::chaincode::NewChaincodeByName("transfer", std::move(orm));
        chaincode->invoke("init", ParamToString({"100"}));
        auto [reads, writes] = chaincode->reset();
        ASSERT_TRUE(reads->empty());
        ASSERT_TRUE(writes->size() == 100);
        ASSERT_TRUE((*writes)[99]->getKeySV() == "99");
        ASSERT_TRUE((*writes)[99]->getValueSV() == "0");
    }

    std::unique_ptr<peer::db::RocksdbConnection> db;
    std::unique_ptr<peer::chaincode::Chaincode> chaincode;

};

TEST_F(ChaincodeTest, TestTransfer) {
    db->syncPut("10", "0");
    db->syncPut("5", "0");
    chaincode->invoke("transfer", ParamToString({"10", "5"}));
    auto [reads, writes] = chaincode->reset();
    ASSERT_TRUE(reads->size() == 2);
    ASSERT_TRUE(writes->size() == 2);
    ASSERT_TRUE((*reads)[0]->getKeySV() == "10");
    ASSERT_TRUE((*reads)[0]->getValueSV() == "0");
    ASSERT_TRUE((*reads)[1]->getKeySV() == "5");
    ASSERT_TRUE((*reads)[1]->getValueSV() == "0");
    ASSERT_TRUE((*writes)[0]->getKeySV() == "10");
    ASSERT_TRUE((*writes)[0]->getValueSV() == "-100");
    ASSERT_TRUE((*writes)[1]->getKeySV() == "5");
    ASSERT_TRUE((*writes)[1]->getValueSV() == "100");
    db->syncDelete("10");
    db->syncDelete("5");

}