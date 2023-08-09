//
// Created by user on 23-8-9.
//

#include<client/smallbank_db.h>
#include "glog/logging.h"
#include "proto/user_request.h"

namespace client{
  SmallBankDB::SmallBankDB() { DLOG(INFO) << "SmallBankDB started."; }

  core::Status SmallBankDB::amalgamate(uint32_t acc1, uint32_t acc2) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc1, acc2))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::AMALGAMATE, data);
  }

  core::Status SmallBankDB::getBalance(uint32_t acc) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::GET_BALANCE, data);
  }
  core::Status SmallBankDB::updateBalance(uint32_t acc, uint32_t amount) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc, amount))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::UPDATE_BALANCE, data);
  }

  core::Status SmallBankDB::updateSaving(uint32_t acc, uint32_t amount) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc, amount))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::UPDATE_SAVING, data);
  }

  core::Status SmallBankDB::sendPayment(uint32_t acc1, uint32_t acc2, unsigned int amount) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc1, acc2, amount))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::SEND_PAYMENT, data);
  }

  core::Status SmallBankDB::writeCheck(uint32_t acc, uint32_t amount) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(acc, amount))) {
      return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::WRITE_CHECK, data);
  }

  core::Status SmallBankDB::sendInvokeRequest(const std::string& funcName,
                                                      const std::string& args) {
    //TODO
    return core::STATUS_OK;
  }
}

