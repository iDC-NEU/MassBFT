//
// Created by user on 23-9-5.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "proto/transaction.h"

#include "peer/chaincode/crdt/crdt_orm.h"
#include "peer/chaincode/crdt/crdt_chaincode.h"

namespace peer::cc::crdt {

    class CRDTWorkerFSM : public WorkerFSM {
    public:
        using TxnListType = std::vector<std::unique_ptr<proto::Transaction>>;
        using ResultType = proto::Transaction::ExecutionResult;

    protected:
        ReceiverState OnDestroy() override {
            return peer::cc::ReceiverState::EXITED;
        }

        ReceiverState OnCreate() override {
            pthread_setname_np(pthread_self(), "aria_worker");
            return peer::cc::ReceiverState::READY;
        }

        ReceiverState OnExecuteTransaction() override {
            for (auto& txn: txnList()) {
                // find the chaincode using ccList
                auto ccNameSV = txn->getUserRequest().getCCNameSV();
                auto* chaincode = createOrGetChaincode(ccNameSV);
                auto& userRequest = txn->getUserRequest();
                auto ret = chaincode->InvokeChaincode(userRequest.getFuncNameSV(), userRequest.getArgs());
                // get the rwSets out of the orm
                txn->setRetValue(chaincode->reset());
                // 1. transaction internal error, abort it without adding reserve table
                if (ret != 0) {
                    txn->setExecutionResult(ResultType::ABORT_NO_RETRY);
                    continue;
                }
                txn->setExecutionResult(ResultType::COMMIT);
            }
            // DLOG(INFO) << "Finished execution, id: " << id;
            return peer::cc::ReceiverState::FINISH_EXEC;
        }

        ReceiverState OnCommitTransaction() override {
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }

    protected:
        inline peer::crdt::chaincode::CrdtChaincode* createOrGetChaincode(std::string_view ccNameSV) {
            auto it = ccList.find(ccNameSV);
            if (it == ccList.end()) {   // chaincode not found
                auto orm = std::make_unique<peer::crdt::chaincode::CrdtORM>(db);
                auto ret = peer::crdt::chaincode::NewChaincodeByName(ccNameSV, std::move(orm));
                CHECK(ret != nullptr) << "chaincode name not exist!";
                auto& rawPointer = *ret;
                ccList[ccNameSV] = std::move(ret);
                return &rawPointer;
            } else {
                return it->second.get();
            }
        }

    public:
        inline void setDBShim(std::shared_ptr<peer::crdt::chaincode::DBShim> db_) { db = std::move(db_); }

        [[nodiscard]] TxnListType& getMutableTxnList() { return _txnList; }

    protected:
        [[nodiscard]] TxnListType& txnList() {
            DLOG_IF(WARNING, _txnList.empty()) << "txnList is empty.";
            return _txnList;
        }

    private:
        TxnListType _txnList;
        util::MyFlatHashMap<std::string, std::unique_ptr<peer::crdt::chaincode::CrdtChaincode>> ccList;
        std::shared_ptr<peer::crdt::chaincode::DBShim> db;
    };
}