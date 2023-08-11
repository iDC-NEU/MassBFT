//
// Created by user on 23-3-7.
//

#pragma once

#include "peer/concurrency_control/deterministic/chaincode_worker_fsm.h"
#include "peer/concurrency_control/deterministic/write_based/wb_reserve_table.h"

namespace peer::cc {
    class WBWorkerFSM : public CCWorkerFSM {
    public:
        ReceiverState OnCreate() override {
            pthread_setname_np(pthread_self(), "wb_worker");
            return peer::cc::ReceiverState::READY;
        }

        ReceiverState OnExecuteTransaction() override {
            DCHECK(getDB() != nullptr && reserveTable != nullptr);
            for (auto& txn: txnList()) {
                // find the chaincode using ccList
                auto ccNameSV = txn->getUserRequest().getCCNameSV();
                auto* chaincode = createOrGetChaincode(ccNameSV);
                auto& userRequest = txn->getUserRequest();
                auto ret = chaincode->InvokeChaincode(userRequest.getFuncNameSV(), userRequest.getArgs());
                // get the rwSets out of the orm
                txn->setRetValue(chaincode->reset(txn->getReads(), txn->getWrites()));
                // 1. transaction internal error, abort it without adding reserve table
                if (ret != 0) {
                    txn->setExecutionResult(ResultType::ABORT_NO_RETRY);
                    continue;
                }
                // read only optimization
                if (txn->getWrites().empty()) {
                    continue;
                }
                // 2. reserve rw set
                reserveTable->reserveWrites(txn->getWrites(), txn->getTransactionIdPtr());
            }
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

        void setReserveTable(std::shared_ptr<WBReserveTable> reserveTable_) { reserveTable = std::move(reserveTable_); }

    protected:
        ReceiverState onFirstCommit() {
            for (auto& txn: txnList()) {
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
            auto saveToDBFunc = [&](db::DBConnection::WriteBatch* batch) {
                auto updateDBCallback = [batch](std::string_view keySV, std::string_view valueSV) {
                    if (valueSV.empty()) {
                        batch->Delete({keySV.data(), keySV.size()});
                    } else {
                        batch->Put({keySV.data(), keySV.size()}, {valueSV.data(), valueSV.size()});
                    }
                };

                for (auto& txn: txnList()) {
                    // 1. txn internal error, abort it without dealing with reserve table
                    auto result = txn->getExecutionResult();
                    if (result == ResultType::ABORT_NO_RETRY || result == ResultType::ABORT) {
                        continue;
                    }
                    reserveTable->updateDB(txn->getWrites(), txn->getTransactionId(), updateDBCallback);
                }
                return true;
            };
            if (!getDB()->syncWriteBatch(saveToDBFunc)) {
                LOG(ERROR) << "WorkerFSMImpl can not write to db!";
            }
            // DLOG(INFO) << "Finished commit, id: " << id;
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

    private:
        bool firstCommit = true;
        std::shared_ptr<WBReserveTable> reserveTable;
    };

}