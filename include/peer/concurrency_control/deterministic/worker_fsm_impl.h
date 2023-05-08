//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "peer/concurrency_control/deterministic/reserve_table.h"

#include "peer/db/rocksdb_connection.h"
#include "peer/chaincode/orm.h"
#include "peer/chaincode/chaincode.h"

#include "common/phmap.h"

#include "proto/transaction.h"

namespace peer::cc {

    class WorkerFSMImpl : public WorkerFSM {
    public:
        using TxnListType=std::vector<std::unique_ptr<proto::Transaction>>;
        using ResultType = proto::Transaction::ExecutionResult;
        ReceiverState OnCreate() override {
            pthread_setname_np(pthread_self(), "aria_worker");
            return peer::cc::ReceiverState::READY;
        }

        ReceiverState OnDestroy() override {
            return peer::cc::ReceiverState::EXITED;
        }

        ReceiverState OnExecuteTransaction() override {
            CHECK(db != nullptr) << "failed to init db!";
            util::MyFlatHashMap<std::string, std::unique_ptr<peer::chaincode::Chaincode>> ccList;
            do {    // defer func
                if (txnList.empty() || reserveTable == nullptr) {
                    LOG(WARNING) << "OnExecuteTransaction input error";
                    break;
                }
                for (auto& txn: txnList) {
                    // find the chaincode using ccList
                    auto ccNameSV = txn->getUserRequest().getCCNameSV();
                    auto it = ccList.find(ccNameSV);
                    if (it == ccList.end()) {   // chaincode not found
                        auto ccName = std::string(ccNameSV);
                        auto orm = peer::chaincode::ORM::NewORMFromLeveldb(db.get());
                        auto ret = peer::chaincode::NewChaincodeByName(ccName, std::move(orm));
                        CHECK(ret != nullptr) << "chaincode name not exist!";
                        ccList[ccName] = std::move(ret);
                        it = ccList.find(ccNameSV);
                    }
                    auto chaincode = it->second.get();
                    chaincode->setTxnRawPointer(txn.get());
                    auto& userRequest = txn->getUserRequest();
                    auto ret = chaincode->invoke(userRequest.getFuncNameSV(), userRequest.getArgs());
                    // get the rwSets out of the orm
                    auto [reads, writes] = chaincode->reset();
                    txn->getReads() = std::move(*reads);
                    txn->getWrites() = std::move(*writes);
                    // 1. transaction internal error, abort it without adding reserve table
                    if (ret != 0) {
                        txn->setExecutionResult(ResultType::ABORT_NO_RETRY);
                        continue;
                    }
                    // for committed transactions, read only optimization
                    if (txn->getWrites().empty()) {
                        txn->setExecutionResult(ResultType::COMMIT);
                        continue;
                    }
                    // 2. reserve rw set
                    reserveTable->reserveRWSets(txn->getReads(), txn->getWrites(), txn->getTransactionIdPtr());
                }

            } while(false);
            // DLOG(INFO) << "Finished execution, id: " << id;
            return peer::cc::ReceiverState::FINISH_EXEC;
        }

        ReceiverState OnCommitTransaction() override {
            auto saveToDBFunc = [&](rocksdb::WriteBatch* batch) {
                for (auto& txn: txnList) {
                    // 1. txn internal error, abort it without dealing with reserve table
                    auto result = txn->getExecutionResult();
                    if (result == ResultType::ABORT_NO_RETRY) {
                        continue;
                    }
                    // for committed transactions, read only optimization
                    if (txn->getWrites().empty()) {
                        txn->setExecutionResult(ResultType::COMMIT);
                        continue;
                    }
                    // 2. analyse dependency
                    auto dep = reserveTable->analysisDependent(txn->getReads(), txn->getWrites(), txn->getTransactionId());
                    if (dep.waw) { // waw, abort the txn.
                        txn->setExecutionResult(ResultType::ABORT);
                        continue;
                    } else if (!dep.war || !dep.raw) {    //  war / raw / no dependency, commit it.
                        for (const auto& kv: txn->getWrites()) {
                            auto& keySV = kv->getKeySV();
                            auto& valueSV = kv->getValueSV();
                            if (valueSV.empty()) {
                                batch->Delete({keySV.data(), keySV.size()});
                            } else {
                                batch->Put({keySV.data(), keySV.size()}, {valueSV.data(), valueSV.size()});
                            }
                        }
                        txn->setExecutionResult(ResultType::COMMIT);
                    } else {    // war and raw, abort the txn.
                        txn->setExecutionResult(ResultType::ABORT);
                    }
                }
                return true;
            };
            if (!db->syncWriteBatch(saveToDBFunc)) {
                LOG(ERROR) << "WorkerFSMImpl can not write to db!";
            }
            // DLOG(INFO) << "Finished commit, id: " << id;
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

        [[nodiscard]] TxnListType& getMutableTxnList() { return txnList; }

        void setReserveTable(std::shared_ptr<ReserveTable> reserveTable_) { reserveTable = std::move(reserveTable_); }

        void setDB(std::shared_ptr<db::RocksdbConnection> db_) { db = std::move(db_); }

    private:
        TxnListType txnList;
        // Do not use a shared pointer
        std::shared_ptr<ReserveTable> reserveTable;
        // TODO: use hash map for multiple tables
        std::shared_ptr<db::RocksdbConnection> db;
    };

}