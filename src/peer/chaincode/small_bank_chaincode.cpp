//
// Created by user on 23-8-9.
//
#include "peer/chaincode/small_bank_chaincode.h"
#include "client/small_bank/small_bank_property.h"
#include "client/core/common/byte_iterator.h"
#include "client/core/common/random_double.h"

namespace peer::chaincode {
    using namespace client::small_bank;
    SmallBankChaincode::SmallBankChaincode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) {
        // prepare function map
        functionMap[InvokeRequestType::BALANCE] = [this](std::string_view argSV) { return this->balance(argSV); };
        functionMap[InvokeRequestType::DEPOSIT_CHECKING] = [this](std::string_view argSV) { return this->depositChecking(argSV); };
        functionMap[InvokeRequestType::TRANSACT_SAVING] = [this](std::string_view argSV) { return this->transactSavings(argSV); };
        functionMap[InvokeRequestType::AMALGAMATE] = [this](std::string_view argSV) { return this->amalgamate(argSV); };
        functionMap[InvokeRequestType::WRITE_CHECK] = [this](std::string_view argSV) { return this->writeCheck(argSV); };
    }

    int SmallBankChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        auto it = functionMap.find(funcNameSV);
        if (it == functionMap.end()) {
            LOG(WARNING) << "Function not found!";
            return -1;
        }
        if (it->second(argSV)) {
            return 0;
        }
        return -1;
    }

    int SmallBankChaincode::InitDatabase() {
        ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto sbp = SmallBankProperties::NewFromProperty(*property);

        auto balanceGenerator = client::utils::RandomDouble(StaticConfig::MIN_BALANCE, StaticConfig::MAX_BALANCE);
        for (AccountIDType acctId = 0; acctId < sbp->getAccountsCount(); acctId++) {
            std::string acctName = client::utils::RandomString(StaticConfig::Account_NAME_LENGTH);
            if (!insertIntoTable(TableNamesPrefix::ACCOUNTS, acctId, acctName)) {
                return -1;
            }
            double checkingBalance = balanceGenerator.nextValue();
            double savingsBalance = balanceGenerator.nextValue();
            if (!insertIntoTable(TableNamesPrefix::CHECKING, acctId, checkingBalance)) {
                return -1;
            }
            if (!insertIntoTable(TableNamesPrefix::SAVINGS, acctId, savingsBalance)) {
                return -1;
            }
        }
        return 0;
    }

    bool SmallBankChaincode::balance(std::string_view argSV) {
        zpp::bits::in in(argSV);
        client::small_bank::AccountIDType acctId;
        if (failure(in(acctId))) {
            return false;
        }
        std::string_view accountName;
        if (!getValue(TableNamesPrefix::ACCOUNTS, acctId, accountName)) {
            return false;  // can not find the account
        }
        double savingsBalance, checkingBalance;
        if (!getValue(TableNamesPrefix::SAVINGS, acctId, savingsBalance)) {
            return false;
        }
        if (!getValue(TableNamesPrefix::CHECKING, acctId, checkingBalance)) {
            return false;
        }
        // It returns the sum of savings and checking balances for the specified customer
        std::string ret;
        zpp::bits::out out(ret);
        if (failure(out(acctId, savingsBalance + checkingBalance))) {
            return false;
        }
        orm->setResult(std::move(ret));
        return true;
    }

    bool SmallBankChaincode::depositChecking(std::string_view argSV) {
        zpp::bits::in in(argSV);
        client::small_bank::AccountIDType acctId;
        double amount;
        if (failure(in(acctId, amount))) {
            return false;
        }
        // If the value V is negative or if the name N is not found in the table, the transaction will roll back.
        if (amount < 0) {
            return false;
        }
        std::string_view accountName;
        if (!getValue(TableNamesPrefix::ACCOUNTS, acctId, accountName)) {
            return false;  // can not find the account
        }
        double balance;
        if (!getValue(TableNamesPrefix::CHECKING, acctId, balance)) {
            return false;
        }
        balance += amount;
        if (!insertIntoTable(TableNamesPrefix::CHECKING, acctId, balance)) {
            return false;
        }
        return true;
    }

    bool SmallBankChaincode::transactSavings(std::string_view argSV) {
        // It increases or decreases the savings balance by V for the specified customer.
        zpp::bits::in in(argSV);
        client::small_bank::AccountIDType acctId;
        double amount;
        if (failure(in(acctId, amount))) {
            return false;
        }
        std::string_view accountName;
        if (!getValue(TableNamesPrefix::ACCOUNTS, acctId, accountName)) {
            return false;  // can not find the account
        }
        double balance;
        if (!getValue(TableNamesPrefix::SAVINGS, acctId, balance)) {
            return false;
        }
        // if the transaction would result in a negative savings balance for the customer, the transaction will roll back.
        balance += amount;
        if (balance < 0) {
            return false;
        }
        if (!insertIntoTable(TableNamesPrefix::SAVINGS, acctId, balance)) {
            return false;
        }
        return true;
    }

    bool SmallBankChaincode::amalgamate(std::string_view argSV) {
        zpp::bits::in in(argSV);
        client::small_bank::AccountIDType accId0, accId1;
        if (failure(in(accId0, accId1))) {
            return false;
        }
        std::string_view accountName0, accountName1;
        if (!getValue(TableNamesPrefix::ACCOUNTS, accId0, accountName0)) {
            return false;  // can not find the account
        }
        if (!getValue(TableNamesPrefix::ACCOUNTS, accId1, accountName1)) {
            return false;  // can not find the account
        }
        // It reads the balances for both accounts of customer N1
        double savings0;
        if (!getValue(TableNamesPrefix::SAVINGS, accId0, savings0)) {
            return false;
        }
        double checking0;
        if (!getValue(TableNamesPrefix::CHECKING, accId0, checking0)) {
            return false;
        }
        // then sets both to zero
        if (!insertIntoTable(TableNamesPrefix::SAVINGS, accId0, double(0))) {
            return false;
        }
        if (!insertIntoTable(TableNamesPrefix::CHECKING, accId0, double(0))) {
            return false;
        }
        // and finally increases the checking balance for N2 by the sum of N1’s previous balances.
        double checking1;
        if (!getValue(TableNamesPrefix::CHECKING, accId1, checking1)) {
            return false;
        }
        checking1 += savings0 + checking0;
        if (!insertIntoTable(TableNamesPrefix::CHECKING, accId1, checking1)) {
            return false;
        }
        return true;
    }

    bool SmallBankChaincode::writeCheck(std::string_view argSV) {
        zpp::bits::in in(argSV);
        client::small_bank::AccountIDType acctId;
        double amount;
        if (failure(in(acctId, amount))) {
            return false;
        }
        std::string_view accountName;
        if (!getValue(TableNamesPrefix::ACCOUNTS, acctId, accountName)) {
            return false;  // can not find the account
        }
        // evaluate the sum of savings and checking balances for the given customer
        double savings;
        if (!getValue(TableNamesPrefix::SAVINGS, acctId, savings)) {
            return false;
        }
        double checking;
        if (!getValue(TableNamesPrefix::CHECKING, acctId, checking)) {
            return false;
        }
        double sum = checking + savings;
        // If the sum is less than V, it decreases the checking balance by V + 1 (reflecting a penalty of $1 for overdrawing)
        if (sum < amount) {
            checking -= amount + 1;
        } else {    // otherwise it decreases the checking balance by V.
            checking -= amount;
        }
        if (!insertIntoTable(TableNamesPrefix::CHECKING, acctId, checking)) {
            return false;
        }
        return true;
    }

    template<class Key, class Value>
    bool SmallBankChaincode::getValue(std::string_view tablePrefix, const Key &key, Value &value) {
        std::string keyRaw(tablePrefix);
        zpp::bits::out outKey(keyRaw);
        outKey.reset(keyRaw.size());
        if(failure(outKey(key))) {
            return false;
        }
        std::string_view valueSV;
        if (!orm->get(std::move(keyRaw), &valueSV)) {
            return false;
        }
        zpp::bits::in inValue(valueSV);
        if(failure(inValue(value))) {
            return false;
        }
        return true;
    }

    template<class Key, class Value>
    bool SmallBankChaincode::insertIntoTable(std::string_view tablePrefix, const Key &key, const Value &value) {
        std::string keyRaw(tablePrefix);
        zpp::bits::out outKey(keyRaw);
        outKey.reset(keyRaw.size());
        if(failure(outKey(key))) {
            return false;
        }
        std::string valueRaw;
        zpp::bits::out outValue(valueRaw);
        if(failure(outValue(value))) {
            return false;
        }
        orm->put(std::move(keyRaw), std::move(valueRaw));
        return true;
    }
}