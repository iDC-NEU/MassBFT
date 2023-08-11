//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/concurrency_control/deterministic/chaincode_worker_fsm.h"
#include "peer/concurrency_control/deterministic/reserve_table.h"

namespace peer::cc {
    class WorkerFSMImpl : public CCWorkerFSM {
    public:
        ReceiverState OnCreate() override {
            pthread_setname_np(pthread_self(), "aria_worker");
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
                // for committed transactions, read only optimization
                if (txn->getWrites().empty()) {
                    txn->setExecutionResult(ResultType::COMMIT);
                    continue;
                }
                // 2. reserve rw set
                reserveTable->reserveRWSets(txn->getReads(), txn->getWrites(), txn->getTransactionIdPtr());
            }
            // DLOG(INFO) << "Finished execution, id: " << id;
            return peer::cc::ReceiverState::FINISH_EXEC;
        }

        ReceiverState OnCommitTransaction() override {
            auto saveToDBFunc = [&](auto* batch) {
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
            if (!getDB()->syncWriteBatch(saveToDBFunc)) {
                LOG(ERROR) << "WorkerFSMImpl can not write to db!";
            }
            // DLOG(INFO) << "Finished commit, id: " << id;
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

        void setReserveTable(std::shared_ptr<ReserveTable> reserveTable_) { reserveTable = std::move(reserveTable_); }

    private:
        std::shared_ptr<ReserveTable> reserveTable;
    };
}