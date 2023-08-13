//
// Created by user on 23-8-9.
//

#pragma once

#include "peer/chaincode/chaincode.h"
#include "client/small_bank/small_bank_helper.h"
#include "common/phmap.h"

namespace peer::chaincode {
    class SmallBankChaincode : public Chaincode {
    public:
        using FunctionType = std::function<bool(std::string_view argSV)>;

        explicit SmallBankChaincode(std::unique_ptr<ORM> orm_);

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

    protected:
        bool balance(std::string_view argSV);

        bool depositChecking(std::string_view argSV);

        bool transactSavings(std::string_view argSV);

        bool amalgamate(std::string_view argSV);

        bool writeCheck(std::string_view argSV);

    protected:
        template<class Key, class Value>
        inline bool insertIntoTable(std::string_view tablePrefix, const Key& key, const Value& value);

        template<class Key, class Value>
        inline bool getValue(std::string_view tablePrefix, const Key& key, Value& value);

    private:
        util::MyFlatHashMap<std::string_view, FunctionType> functionMap;
    };
}
