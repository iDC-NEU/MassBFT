//
// Created by peng on 2/22/23.
//

#include "peer/concurrency_control/deterministic/write_based/wb_coordinator.h"
#include "tests/transaction_utils.h"

#include "common/timer.h"

#include "gtest/gtest.h"
#include "glog/logging.h"

class WBCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

};


TEST_F(WBCoordinatorTest, TestSignalSendReceive) {
    constexpr int recordCount=100000;
    std::shared_ptr<peer::db::RocksdbConnection> dbc = peer::db::RocksdbConnection::NewConnection("testDB");
    CHECK(dbc != nullptr) << "create db failed!";
    // init db
    dbc->syncWriteBatch([](rocksdb::WriteBatch* batch){
        for (int i=0; i<recordCount; i++) {
            batch->Put(std::to_string(i), "0");
        }
        return true;
    });
    auto c = peer::cc::WBCoordinator::NewCoordinator(dbc, 10);

    std::vector<std::vector<std::unique_ptr<proto::Transaction>>> txnListList;
    txnListList.resize(100);
    for (int i=0; i<100; i++) {
        tests::TransactionUtils::CreateMockTxn(&txnListList[i], 3000, recordCount);
    }
    util::Timer timer;
    int totalCommit=0;
    int totalAbort=0;
    for (int i=0; i<100; i++) {
        LOG(INFO) << "Round " << i;
        auto ret = c->processTxnList(txnListList[i]);
        ASSERT_TRUE(ret);
        for (const auto& it: txnListList[i]) {
            if (it->getExecutionResult() == proto::Transaction::ExecutionResult::COMMIT) {
                totalCommit++;
            } else {
                totalAbort++;
            }
        }
    }
    auto cost = timer.end();
    // validate result
    int totalValue = 0;
    for (int i=0; i < recordCount; i++) {
        std::string value;
        dbc->get(std::to_string(i), &value);
        totalValue += std::atoi(value.data());
    }
    ASSERT_TRUE(totalValue == 0);
    LOG(INFO) << "Total commit: " << totalCommit;
    LOG(INFO) << "Total abort: " << totalAbort;
    LOG(INFO) << "Tps: " << totalCommit/cost;
}