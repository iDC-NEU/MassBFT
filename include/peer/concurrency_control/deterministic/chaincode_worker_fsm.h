//
// Created by user on 23-8-10.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "proto/transaction.h"

#include "peer/chaincode/orm.h"
#include "peer/chaincode/chaincode.h"

namespace peer::cc {
    class CCWorkerFSM : public WorkerFSM {
    public:
        using TxnListType = std::vector<std::unique_ptr<proto::Transaction>>;
        using ResultType = proto::Transaction::ExecutionResult;

        ReceiverState OnDestroy() override {
            return peer::cc::ReceiverState::EXITED;
        }

    protected:
        inline peer::chaincode::Chaincode* createOrGetChaincode(std::string_view ccNameSV) {
            auto it = ccList.find(ccNameSV);
            if (it == ccList.end()) {   // chaincode not found
                auto orm = peer::chaincode::ORM::NewORMFromDBInterface(db);
                auto ret = peer::chaincode::NewChaincodeByName(ccNameSV, std::move(orm));
                CHECK(ret != nullptr) << "chaincode name not exist!";
                auto& rawPointer = *ret;
                ccList[ccNameSV] = std::move(ret);
                return &rawPointer;
            } else {
                return it->second.get();
            }
        }

    public:
        inline void setDB(std::shared_ptr<db::DBConnection> db_) { db = std::move(db_); }

        [[nodiscard]] inline db::DBConnection* getDB() const { return db.get(); }

        [[nodiscard]] TxnListType& getMutableTxnList() { return _txnList; }

    protected:
        [[nodiscard]] TxnListType& txnList() {
            DLOG_IF(WARNING, _txnList.empty()) << "txnList is empty.";
            return _txnList;
        }

    private:
        TxnListType _txnList;
        util::MyFlatHashMap<std::string, std::unique_ptr<peer::chaincode::Chaincode>> ccList;
        std::shared_ptr<db::DBConnection> db;
    };
}