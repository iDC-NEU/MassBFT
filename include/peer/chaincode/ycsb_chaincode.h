//
// Created by user on 23-6-30.
//

#pragma once

#include "chaincode.h"

namespace peer::chaincode {
    class YCSBChaincode : public Chaincode {
    public:
        explicit YCSBChaincode(std::unique_ptr<ORM> orm);

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

        inline static std::string BuildKeyField(auto&& key, auto&& field) {
            return std::string(key).append("_").append(field);
        }

    protected:
        int update(std::string_view argSV);

        int insert(std::string_view argSV) { return update(argSV); }

        int read(std::string_view argSV);

        int remove(std::string_view argSV);

        int scan(std::string_view argSV);

        int readModifyWrite(std::string_view argSV);

    private:
        std::vector<std::string> fieldNames;
    };
}
