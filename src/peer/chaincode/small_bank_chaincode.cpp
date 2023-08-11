//
// Created by user on 23-8-9.
//
#include "peer/chaincode/small_bank_chaincode.h"
#include "client/smallbank/smallbank_property.h"

peer::chaincode::SmallBankChaincode::SmallBankChaincode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) {
    //TODO: init

}

int peer::chaincode::SmallBankChaincode::InvokeChaincode(std::string_view funcNameSV,
                                                         std::string_view argSV) {
    zpp::bits::in in(argSV);
    // acc means account
    std::string_view acc, from, to, amount;
    if (funcNameSV == "amalgamate") {
        if (failure(in(from, to))) {
            return -1;
        }
        return this->amalgamate(from, to);
    }
    if (funcNameSV == "getBalance") {
        if (failure(in(acc))) {
            return -1;
        }
        return this->query(acc);
    }
    if (funcNameSV == "updateBalance") {
        if (failure(in(from, amount))) {
            return -1;
        }
        return this->updateBalance(from, amount);
    }
    if (funcNameSV == "updateSaving") {
        if (failure(in(from, amount))) {
            return -1;
        }
        return this->updateSaving(from, amount);
    }
    if (funcNameSV == "sendPayment") {
        if (failure(in(from, to, amount))) {
            return -1;
        }
        return this->sendPayment(from, to, amount);
    }
    if (funcNameSV == "writeCheck") {
        if (failure(in(from, amount))) {
            return -1;
        }
        return this->writeCheck(from, amount);
    }
    return 0;
}

int peer::chaincode::SmallBankChaincode::InitDatabase() {
    // TODO:Random simulation inserting user account amount
    // randomly insert into database smallBank: checking and saving respectively
//    Random random;
//    for(int i = 0; i < 10000; i++) {
//        AriaORM::ORMInsert* insert = helper->newInsert("small_bank");
//        insert->set("key", checkingTab + "_" + std::to_string(i));
//        insert->set("value", std::to_string(random.next()%100));
//    }
//
//    for(int i = 0; i < 10000; i++) {
//        AriaORM::ORMInsert* insert = helper->newInsert("small_bank");
//        insert->set("key", savingTab + "_" + std::to_string(i));
//        insert->set("value", std::to_string(random.next()%100));
//    }
//    return true;
}

int peer::chaincode::SmallBankChaincode::query(const std::string_view &acc) {
    int bal1 = Get(std::string(client::smallbank::SMALLBANKProperties::SAVING_TAB) + "_" + std::string(acc));
    int bal2 = Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(acc));

    DLOG(INFO) << "query account:" << acc <<", savingTab: " << bal1  <<", checkingTab: " << bal2;
    return true;
}

int peer::chaincode::SmallBankChaincode::amalgamate(const std::string_view &from,
                                                    const std::string_view &to) {
    if(from == to){
        LOG(INFO) <<"the transfer accounts (in and out) are the same.";
        return false;
    }
    int sav_bal1= Get(std::string(client::smallbank::SMALLBANKProperties::SAVING_TAB)  + "_" + std::string(from));
    int che_bal2 = Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(to));

    Set(std::string(client::smallbank::SMALLBANKProperties::SAVING_TAB)  + "_" + std::string(from), std::to_string(0));
    Set(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(to), std::to_string(sav_bal1 + che_bal2));
    return true;
}

int peer::chaincode::SmallBankChaincode::updateBalance(const std::string_view &acc,
                                                       const std::string_view &amount) {
    int balance = Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(acc));
    int transfer = std::stoi(std::string(amount));
    if (transfer < 0) {
        return false;
    }
    Set(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(acc), std::to_string(balance+transfer));
    return true;
}

int peer::chaincode::SmallBankChaincode::updateSaving(const std::string_view &acc,
                                                      const std::string_view &amount) {
    int balance = Get(std::string(client::smallbank::SMALLBANKProperties::SAVING_TAB) + "_" + std::string(acc));
    int transfer = std::stoi(std::string(amount));
    if (transfer < 0) {
        return false;
    }

    Set(std::string(client::smallbank::SMALLBANKProperties::SAVING_TAB) + "_" + std::string(acc), std::to_string(balance+transfer));
    return true;
}

int peer::chaincode::SmallBankChaincode::sendPayment(const std::string_view &from, const std::string_view &to,
                                                     const std::string_view &amount) {
    if(from == to){
        LOG(INFO) <<"the transfer accounts (in and out) are the same.";
        return false;
    }

    int bal1= Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(from));
    int bal2 = Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(to));
    int transfer = std::stoi(std::string(amount));

    bal1 -= transfer;
    if (bal1 < 0 || transfer< 0)
        return false;
    bal2 += transfer;

    Set(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(from), std::to_string(bal1));
    Set(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(to), std::to_string(bal2));
    return true;
}

int peer::chaincode::SmallBankChaincode::writeCheck(const std::string_view &from,
                                                    const std::string_view &amount) {
    // TODO: have confusion with this function
    int bal1 = Get(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(from));
    int transfer = std::stoi(std::string(amount));

    bal1 -= transfer;
    if(transfer < 0){
        return false;
    }

    Set(std::string(client::smallbank::SMALLBANKProperties::CHECKING_TAB) + "_" + std::string(from), std::to_string(bal1));
    return true;
}
