//
// Created by user on 23-3-7.
//
#pragma once

#include "tests/transaction_utils.h"
#include "peer/db/db_interface.h"
#include "common/timer.h"
#include "common/property.h"

#include "glog/logging.h"

namespace tests {
    class CoordinatorUtils {
    public:
        static auto initDB(int recordCount=100000) {
            std::shared_ptr<peer::db::DBConnection> dbc = peer::db::DBConnection::NewConnection("testDB");
            CHECK(dbc != nullptr) << "create db failed!";
            // init db
            dbc->syncWriteBatch([&recordCount](peer::db::DBConnection::WriteBatch* batch){
                for (int i=0; i<recordCount; i++) {
                    batch->Put(std::to_string(i), "0");
                }
                return true;
            });
            return dbc;
        }

        using TxnListType = std::vector<std::unique_ptr<proto::Transaction>>;
        static void StartBenchmark(const std::function<bool(TxnListType&)>& coordinatorCallback,
                                   int recordCount=100000) {
            std::vector<TxnListType> txnListList;
            txnListList.resize(100);
            for (int i=0; i<100; i++) {
                tests::TransactionUtils::CreateMockTxn(&txnListList[i], 3000, recordCount);
            }
            util::Timer timer;
            int totalCommit=0;
            int totalAbort=0;
            for (int i=0; i<100; i++) {
                LOG(INFO) << "Round " << i;
                auto ret = coordinatorCallback(txnListList[i]);
                CHECK(ret) << "Coordinator return false";
                for (const auto& it: txnListList[i]) {
                    if (it->getExecutionResult() == proto::Transaction::ExecutionResult::COMMIT) {
                        totalCommit++;
                    } else {
                        totalAbort++;
                    }
                }
            }
            auto cost = timer.end();
            LOG(INFO) << "Total commit: " << totalCommit;
            LOG(INFO) << "Total abort: " << totalAbort;
            LOG(INFO) << "Tps: " << totalCommit/cost;
        }
    };
}