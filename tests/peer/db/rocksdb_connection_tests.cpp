//
// Created by peng on 2/21/23.
//

#include "peer/db/rocksdb_connection.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class RocksDBCTest : public ::testing::Test {
public:
    RocksDBCTest() {
        dbc = peer::db::RocksdbConnection::NewConnection("testDB");
        CHECK(dbc != nullptr) << "create db failed!";
        CHECK(dbc->getDBName() == "testDB") << "create db failed!";
    }
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
    std::unique_ptr<peer::db::RocksdbConnection> dbc;
};

TEST_F(RocksDBCTest, TestGetPutDelete) {
    auto key = "testKey";
    std::string value;
    // async version
    ASSERT_TRUE(dbc->asyncPut(key, "testValue"));
    ASSERT_TRUE(dbc->get(key, &value));
    ASSERT_TRUE(value == "testValue");
    ASSERT_TRUE(dbc->asyncDelete(key));
    ASSERT_TRUE(!dbc->get(key, &value)) << "get after delete!";

    // sync version
    ASSERT_TRUE(dbc->syncPut(key, "testValue"));
    ASSERT_TRUE(dbc->get(key, &value));
    ASSERT_TRUE(value == "testValue");
    ASSERT_TRUE(dbc->syncDelete(key));
    ASSERT_TRUE(!dbc->get(key, &value)) << "get after delete!";

    // batch
    auto ret = dbc->syncWriteBatch([&](rocksdb::WriteBatch* batch){
        batch->Put(key, "testValue");
        return true;
    });
    ASSERT_TRUE(ret);
    ASSERT_TRUE(dbc->get(key, &value));
    ASSERT_TRUE(value == "testValue");
    ret = dbc->syncWriteBatch([&](rocksdb::WriteBatch* batch){
        batch->Delete(key);
        return true;
    });
    ASSERT_TRUE(ret);
    ASSERT_TRUE(!dbc->get(key, &value)) << "get after delete!";
}
