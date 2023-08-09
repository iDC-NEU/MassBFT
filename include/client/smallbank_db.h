//
// Created by user on 23-8-9.
//

#ifndef NBP_SMALLBANK_DB_H
#define NBP_SMALLBANK_DB_H

#include <cstdint>
#include <memory>
#include <vector>
#include "client/core/status.h"

namespace client{
  class SmallBankDB {
  public:
    struct InvokeRequestType {
      constexpr static const auto SMALL_BANK = "smallBank";
      constexpr static const auto AMALGAMATE = "amalgamate";
      constexpr static const auto GET_BALANCE = "getBalance";
      constexpr static const auto UPDATE_BALANCE = "updateBalance";
      constexpr static const auto UPDATE_SAVING = "updateSaving";
      constexpr static const auto SEND_PAYMENT = "sendPayment";
      constexpr static const auto WRITE_CHECK = "writeCheck";
    };

    SmallBankDB();

    core::Status amalgamate(uint32_t  acc1, uint32_t  acc2);
    core::Status getBalance(uint32_t acc);
    core::Status updateBalance(uint32_t acc, uint32_t amount);
    core::Status updateSaving(uint32_t acc, uint32_t amount);
    core::Status sendPayment(uint32_t acc1, uint32_t acc2, unsigned amount);
    core::Status writeCheck(uint32_t acc, uint32_t amount);

  protected:
    core::Status sendInvokeRequest(const std::string& funcName, const std::string& args);
  };
}

#endif  // NBP_SMALLBANK_DB_H