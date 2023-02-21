//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/chaincode/orm.h"
#include "proto/transaction.h"

namespace peer::chaincode {
    template<class Derived>
    class Chaincode {
    public:
        Chaincode(std::unique_ptr<ORM> orm_, const proto::Transaction* txn_)
                : orm(std::move(orm_)), txn(txn_){ }

        ~Chaincode() = default;

        Chaincode(const Chaincode&) = delete;

        Chaincode(Chaincode&&) = delete;

        // return ret code, deserialize argSV
        int invoke(std::string_view ccNameSV, std::string_view argSV) {
            std::vector<std::string_view> args;
            zpp::bits::in in(argSV);
            if (failure(in(args))) {
                LOG(WARNING) << "Chaincode args deserialize failed!";
                return -1;
            }
            return static_cast<Derived*>(this)->InvokeChaincode(ccNameSV, args);
        }

        // reset, return the read write sets
        auto reset() -> std::pair<std::unique_ptr<proto::KVList>, std::unique_ptr<proto::KVList>> {
            return orm->reset();
        }

        void setTxnRawPointer(const proto::Transaction* txn_) { txn=txn_; }

    protected:
        // use orm to write to db
        std::unique_ptr<ORM> orm;
        // get additional information from txn
        const proto::Transaction* txn;
    };
}