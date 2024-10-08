//
// Created by peng on 2/21/23.
//

#include "peer/db/db_interface.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class DBInterfaceTest : public ::testing::Test {
public:
    DBInterfaceTest() {
        dbc = peer::db::DBConnection::NewConnection("testDB");
        CHECK(dbc != nullptr) << "create db failed!";
        CHECK(dbc->getDBName() == "testDB") << "create db failed!";
    }
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
    std::unique_ptr<peer::db::DBConnection> dbc;
};

TEST_F(DBInterfaceTest, TestGetPutDelete) {
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
    auto ret = dbc->syncWriteBatch([&](peer::db::DBConnection::WriteBatch* batch){
        batch->Put(key, "testValue");
        return true;
    });
    ASSERT_TRUE(ret);
    ASSERT_TRUE(dbc->get(key, &value));
    ASSERT_TRUE(value == "testValue");
    ret = dbc->syncWriteBatch([&](peer::db::DBConnection::WriteBatch* batch){
        batch->Delete(key);
        return true;
    });
    ASSERT_TRUE(ret);
    ASSERT_TRUE(!dbc->get(key, &value)) << "get after delete!";
}
