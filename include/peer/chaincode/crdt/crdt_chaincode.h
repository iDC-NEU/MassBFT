//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/chaincode/crdt/crdt_orm.h"

namespace peer::crdt::chaincode {
    class CrdtChaincode {
    public:
        explicit CrdtChaincode(std::unique_ptr<CrdtORM> orm_) : orm(std::move(orm_)) {}

        virtual ~CrdtChaincode() = default;

        CrdtChaincode(const CrdtChaincode &) = delete;

        CrdtChaincode(CrdtChaincode &&) = delete;

        virtual int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) = 0;

        virtual int InitDatabase() { return 0; }

        inline std::string reset() {
            return orm->reset();
        }

    protected:
        // use orm to write to db
        std::unique_ptr<CrdtORM> orm;
    };

    std::unique_ptr<CrdtChaincode> NewChaincodeByName(std::string_view ccName, std::unique_ptr<CrdtORM> orm);
}