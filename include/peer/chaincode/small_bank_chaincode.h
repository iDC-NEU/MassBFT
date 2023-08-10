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

        int Set(std::string_view key, std::string_view value) {
            orm->put(std::string(key), std::string(value));
            return 0;
        }

        // return the value(string_view) instead of 0
        int Get(std::string_view key) {
            std::string_view value;
            if (orm->get(std::string(key), &value)) {
                return std::stoi(std::string(value));
            }
            return -1;
        }

    protected:
        // query a account's amount.
        int query(const std::string_view &acc);

        // transfer the entire contents of one customer's savings account into another customer's checking account.
        // transfer all assets from a to b.
        int amalgamate(const std::string_view &from, const std::string_view &to);

        // add some money to checkingTab of a account
        int updateBalance(const std::string_view &acc, const std::string_view &amount);

        // add some money to savingTab of a account
        int updateSaving(const std::string_view &acc, const std::string_view &amount);

        // send checkingTab a to b
        int sendPayment(const std::string_view &from, const std::string_view &to, const std::string_view &amount);

        // remove an amount from the customer's
        int writeCheck(const std::string_view &from, const std::string_view &amount);

    private:
        const int BALANCE = 1000;
        const std::string savingTab = "saving";
        const std::string checkingTab = "checking";
    };
}
