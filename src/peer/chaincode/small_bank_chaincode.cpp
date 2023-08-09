//
// Created by user on 23-8-9.
//
#include "peer/chaincode/small_bank_chaincode.h"

peer::chaincode::SmallBankChaincode::SmallBankChaincode(std::unique_ptr<ORM> orm): Chaincode(std::move(orm)) {

}
int peer::chaincode::SmallBankChaincode::InvokeChaincode(std::string_view funcNameSV,
                                                         std::string_view argSV) {
  return 0;
}

int peer::chaincode::SmallBankChaincode::InitDatabase() { return Chaincode::InitDatabase(); }

int peer::chaincode::SmallBankChaincode::query(const std::string& account) { return 0; }

int peer::chaincode::SmallBankChaincode::amalgamate(const std::string& from,
                                                    const std::string& to) {
  return 0;
}

int peer::chaincode::SmallBankChaincode::updateBalance(const std::string& from,
                                                       const std::string& val) {
  return 0;
}

int peer::chaincode::SmallBankChaincode::updateSaving(const std::string& from,
                                                      const std::string& val) {
  return 0;
}

int peer::chaincode::SmallBankChaincode::sendPayment(const std::string& from, const std::string& to,
                                                     const std::string& amount) {
  return 0;
}

int peer::chaincode::SmallBankChaincode::writeCheck(const std::string& from,
                                                    const std::string& amountRaw) {
  return 0;
}
