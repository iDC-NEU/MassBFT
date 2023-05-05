//
// Created by user on 23-5-4.
//

#include "ycsb/ycsb_db.h"
#include "glog/logging.h"
#include "proto/tpc-c.pb.h"

struct Request {
    // if empty = test_table
    std::string tableName;
    // if empty = use config file default
    std::string funcName;
    std::vector<std::string> reads;
    std::vector<std::string> writes;
};

ycsb::core::Status ycsb::client::YCSB_DB::read(const std::string &table, const std::string &key,
                                               const std::vector<std::string> &fields, utils::ByteIteratorMap &result) {
    Request request;
    request.funcName = "read";
    request.tableName = table;
    YCSB_FOR_BLOCK_BENCH payload;
    for (const auto& pair: fields){
        auto* value = payload.add_values();
        value->set_key(pair);
        value->set_value("_");
    }
    request.reads = {key, payload.SerializeAsString()};

    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::YCSB_DB::scan(const std::string &table, const std::string &startkey,
                                               uint64_t recordcount, const std::vector<std::string> &fields,
                                               std::vector<utils::ByteIteratorMap> &result) {
    LOG(WARNING) << "YCSB_DB does not support Scan op.";
    // is this return value ok?
    return core::SERVICE_UNAVAILABLE;
}

ycsb::core::Status ycsb::client::YCSB_DB::update(const std::string &table, const std::string &key,
                                                 const utils::ByteIteratorMap &values) {
    Request request;
    request.funcName = "update";
    request.tableName = table;
    YCSB_FOR_BLOCK_BENCH payload;
    for (const auto& pair: values){
        auto* value = payload.add_values();
        value->set_key(pair.first);
        value->set_value(pair.second->toString());
    }
    request.reads = {key, payload.SerializeAsString()};

    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::YCSB_DB::insert(const std::string &table, const std::string &key,
                                                 const utils::ByteIteratorMap &values) {
    return update(table, key, values);
}

ycsb::core::Status ycsb::client::YCSB_DB::remove(const std::string &table, const std::string &key) {
    Request request;
    request.funcName = "remove";
    request.tableName = table;
    request.reads = {key};

    return core::STATUS_OK;
}