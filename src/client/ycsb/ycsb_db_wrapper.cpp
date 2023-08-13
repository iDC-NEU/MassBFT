//
// Created by user on 23-8-10.
//

#include "client/ycsb/ycsb_db_wrapper.h"
#include "client/ycsb/ycsb_helper.h"
#include "client/core/status.h"

namespace client::ycsb {
    core::Status DBWrapper::read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) {
        std::string data;
        zpp::bits::out out(data);
        // 1. table and key
        if (failure(out(table, key))) {
            return core::ERROR;
        }
        // 2. fields
        if (failure(out(fields))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::READ, data);
    }

    core::Status DBWrapper::scan(const std::string &table,
                                  const std::string &startKey,
                                  uint64_t recordCount,
                                  const std::vector<std::string> &fields) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(table, startKey, recordCount, fields))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::SCAN, data);
    }

    core::Status DBWrapper::update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) {
        std::vector<std::pair<std::string, std::string>> args;
        // NOTICE: the first one is table and key
        for(const auto& pair: values) {
            args.emplace_back(pair.first, pair.second);
        }
        std::string data;
        zpp::bits::out out(data);
        // 1. table and key
        if (failure(out(table, key))) {
            return core::ERROR;
        }
        // 2. fields
        if (failure(out(args))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::UPDATE, data);
    }

    core::Status DBWrapper::readModifyWrite(const std::string &table, const std::string &key,
                                             const std::vector<std::string> &readFields,
                                             const utils::ByteIteratorMap &writeValues) {
        std::vector<std::pair<std::string, std::string>> writeArgs;
        // NOTICE: the first one is table and key
        for(const auto& pair: writeValues) {
            writeArgs.emplace_back(pair.first, pair.second);
        }
        std::string data;
        zpp::bits::out out(data);
        // 1. table and key
        if (failure(out(table, key))) {
            return core::ERROR;
        }
        // 2. read fields
        if (failure(out(readFields))) {
            return core::ERROR;
        }
        // 2. write fields
        if (failure(out(writeArgs))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::READ_MODIFY_WRITE, data);
    }

    core::Status DBWrapper::insert(const std::string &table, const std::string &key, const utils::ByteIteratorMap &values) {
        std::vector<std::pair<std::string, std::string>> args;
        // NOTICE: the first one is table and key
        for(const auto& pair: values) {
            args.emplace_back(pair.first, pair.second);
        }
        std::string data;
        zpp::bits::out out(data);
        // 1. table and key
        if (failure(out(table, key))) {
            return core::ERROR;
        }
        // 2. fields
        if (failure(out(args))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::INSERT, data);
    }

    core::Status DBWrapper::remove(const std::string &table, const std::string &key) {
        std::string data;
        zpp::bits::out out(data);
        // only table and key
        if (failure(out(table, key))) {
            return core::ERROR;
        }
        return db->sendInvokeRequest(InvokeRequestType::YCSB, InvokeRequestType::DELETE, data);
    }
}