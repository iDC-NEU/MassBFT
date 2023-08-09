//
// Created by user on 23-8-9.
//

#ifndef NBP_SMALL_BANK_CHAINCODE_H
#define NBP_SMALL_BANK_CHAINCODE_H

#include "chaincode.h"

namespace peer::chaincode {
  class SmallBankChaincode: public Chaincode {
  public:
    explicit SmallBankChaincode(std::unique_ptr<ORM> orm);

    // return ret code
    int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

    int InitDatabase() override;

  protected:
    int query(const std::string& account);
    int amalgamate(const std::string& from, const std::string& to);
    int updateBalance(const std::string& from, const std::string& val);
    int updateSaving(const std::string& from, const std::string& val);
    int sendPayment(const std::string& from, const std::string& to, const std::string& amount);
    int writeCheck(const std::string& from, const std::string& amountRaw);

  private:
    std::function<std::string(const std::string&)> queryLambda;
    std::function<void(const std::string&, int)> updateLambda;

    const int BALANCE = 1000;
    const std::string savingTab = "saving";
    const std::string checkingTab = "checking";
  };
}

#endif  // NBP_SMALL_BANK_CHAINCODE_H
