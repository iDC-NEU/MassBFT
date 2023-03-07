//
// Created by user on 23-3-7.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "peer/concurrency_control/deterministic/write_based/wb_reserve_table.h"

#include "peer/db/rocksdb_connection.h"
#include "peer/chaincode/orm.h"
#include "peer/chaincode/simple_transfer.h"

#include "proto/transaction.h"

namespace peer::cc {

    class WBWorkerFSM : public WorkerFSM {
    public:
        using TxnListType=std::vector<std::unique_ptr<proto::Transaction>>;
        using ResultType = proto::Transaction::ExecutionResult;
        ReceiverState OnCreate() override {
            pthread_setname_np(pthread_self(), "wb_worker");
            return peer::cc::ReceiverState::READY;
        }

        ReceiverState OnDestroy() override {
            return peer::cc::ReceiverState::EXITED;
        }

        ReceiverState OnExecuteTransaction() override {
            CHECK(db != nullptr) << "failed to init db!";
            auto orm = peer::chaincode::ORM::NewORMFromLeveldb(db.get());
            // TODO: use factory method
            auto chaincode = std::make_unique<peer::chaincode::SimpleTransfer>(std::move(orm), nullptr);
            do {    // defer func
                if (txnList.empty() || reserveTable == nullptr) {
                    LOG(WARNING) << "OnExecuteTransaction input error";
                    break;
                }
                for (auto& txn: txnList) {
                    chaincode->setTxnRawPointer(txn.get());
                    auto& userRequest = txn->getUserRequest();
                    auto ret = chaincode->invoke(userRequest.getCCNameSV(), userRequest.getArgs());
                    // get the rwSets out of the orm
                    auto [reads, writes] = chaincode->reset();
                    txn->getReads() = std::move(*reads);
                    txn->getWrites() = std::move(*writes);
                    // 1. transaction internal error, abort it without adding reserve table
                    if ( ret != 0) {
                        txn->setExecutionResult(ResultType::ABORT_NO_RETRY);
                        continue;
                    }
                    // 2. reserve rw set
                    reserveTable->reserveWrites(txn->getWrites(), txn->getTransactionIdPtr());
                }
            } while(false);
            return peer::cc::ReceiverState::FINISH_EXEC;
        }

        ReceiverState OnCommitTransaction() override {
            ReceiverState ret;
            if (firstCommit) {
                ret = onFirstCommit();
            } else {
                ret = onSecondCommit();
            }
            firstCommit = !firstCommit;
            return ret;
        }

        [[nodiscard]] TxnListType& getMutableTxnList() { return txnList; }

        void setReserveTable(std::shared_ptr<WBReserveTable> reserveTable_) { reserveTable = std::move(reserveTable_); }

        void setDB(std::shared_ptr<db::RocksdbConnection> db_) { db = std::move(db_); }

    protected:
        ReceiverState onFirstCommit() {
            for (auto& txn: txnList) {
                // 1. txn internal error, abort it without dealing with reserve table
                auto result = txn->getExecutionResult();
                if (result == ResultType::ABORT_NO_RETRY) {
                    continue;
                }
                // 2. analyse raw
                auto raw = reserveTable->detectRAW(txn->getReads(), txn->getTransactionId());
                if (raw) {  // raw, abort the txn
                    txn->setExecutionResult(ResultType::ABORT);
                    continue;
                }
                // 3. reserve mvcc writes
                reserveTable->mvccReserveWrites(txn->getWrites(), txn->getTransactionIdPtr());
                txn->setExecutionResult(ResultType::COMMIT);
            }
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

        ReceiverState onSecondCommit() {
            auto saveToDBFunc = [&](rocksdb::WriteBatch* batch) {
                auto updateDBCallback = [batch](std::string_view keySV, std::string_view valueSV) {
                    if (valueSV.empty()) {
                        batch->Delete({keySV.data(), keySV.size()});
                    } else {
                        batch->Put({keySV.data(), keySV.size()}, {valueSV.data(), valueSV.size()});
                    }
                };

                for (auto& txn: txnList) {
                    // 1. txn internal error, abort it without dealing with reserve table
                    auto result = txn->getExecutionResult();
                    if (result == ResultType::ABORT_NO_RETRY || result == ResultType::ABORT) {
                        continue;
                    }
                    reserveTable->updateDB(txn->getWrites(), txn->getTransactionId(), updateDBCallback);
                }
                return true;
            };
            if (!db->syncWriteBatch(saveToDBFunc)) {
                LOG(ERROR) << "WorkerFSMImpl can not write to db!";
            }
            // DLOG(INFO) << "Finished commit, id: " << id;
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

    private:
        bool firstCommit = true;
        TxnListType txnList;
        // Do not use a shared pointer
        std::shared_ptr<WBReserveTable> reserveTable;
        // TODO: use hash map for multiple tables
        std::shared_ptr<db::RocksdbConnection> db;
    };

}