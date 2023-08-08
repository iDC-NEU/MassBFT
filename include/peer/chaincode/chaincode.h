//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/chaincode/orm.h"
#include "proto/transaction.h"

namespace peer::chaincode {
    class Chaincode {
    public:
        explicit Chaincode(std::unique_ptr<ORM> orm_) : orm(std::move(orm_)) {}

        virtual ~Chaincode() = default;

        Chaincode(const Chaincode &) = delete;

        Chaincode(Chaincode &&) = delete;

        virtual int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) = 0;

        virtual int InitDatabase() { return 0; }

        std::string reset(proto::KVList& reads_, proto::KVList& writes_) {
            return orm->reset(reads_, writes_);
        }

        void setTxnRawPointer(const proto::Transaction *txn_) { txn = txn_; }

    protected:
        // use orm to write to db
        std::unique_ptr<ORM> orm;
        // get additional information from txn
        const proto::Transaction *txn = nullptr;
    };

    std::unique_ptr<Chaincode> NewChaincodeByName(const std::string &ccName, std::unique_ptr<ORM> orm);
}