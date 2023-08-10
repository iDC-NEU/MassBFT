//
// Created by user on 23-8-9.
//

#pragma once

#include "chaincode.h"

namespace peer::chaincode {
    class SmallBankChaincode : public Chaincode {
    public:
        explicit SmallBankChaincode(std::unique_ptr<ORM> orm_);

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

    protected:
        int query(const std::string_view &account);

        int amalgamate(const std::string_view &from, const std::string_view &to);

        int updateBalance(const std::string_view &from, const std::string_view &val);

        int updateSaving(const std::string_view &from, const std::string_view &val);

        int sendPayment(const std::string_view &from, const std::string_view &to, const std::string_view &amount);

        int writeCheck(const std::string_view &from, const std::string_view &amountRaw);

    private:
        std::function<std::string(const std::string &)> queryLambda;
        std::function<void(const std::string &, int)> updateLambda;

        const int BALANCE = 1000;
        const std::string savingTab = "saving";
        const std::string checkingTab = "checking";
    };
}
