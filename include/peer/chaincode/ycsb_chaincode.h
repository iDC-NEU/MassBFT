//
// Created by user on 23-6-30.
//

#pragma once

#include "chaincode.h"

namespace peer::chaincode {
    class YCSBChainCode : public Chaincode {
    public:
        explicit YCSBChainCode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase(int recordCount);

    protected:
        int update(std::string_view argSV);

        int insert(std::string_view argSV) { return update(argSV); }

        int read(std::string_view argSV);

        int remove(std::string_view argSV);

        int scan(std::string_view argSV);

        int readModifyWrite(std::string_view argSV);

        inline static std::string buildKeyField(auto&& key, auto&& field) {
            return std::string(key).append("_").append(field);
        }
    };
}

