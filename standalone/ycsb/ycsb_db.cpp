//
// Created by user on 23-5-4.
//

#include "common/ycsb_payload.h"
#include "ycsb/ycsb_db.h"
#include "glog/logging.h"
#include "proto/tpc-c.pb.h"

ycsb::core::Status ycsb::client::YCSB_DB::read(const std::string &table, const std::string &key,
                                               const std::vector<std::string> &fields, utils::ByteIteratorMap &result) {
    Utils::Request request;
    request.funcName = "read";
    request.tableName = table;
    YCSB_FOR_BLOCK_BENCH payload;
    for (const auto& pair: fields){
        auto* value = payload.add_values();
        value->set_key(pair);
        value->set_value("_");
    }
    request.reads = {key, payload.SerializeAsString()};
    // sendInvokeRequest(request);
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
    Utils::Request request;
    request.funcName = "update";
    request.tableName = table;
    YCSB_FOR_BLOCK_BENCH payload;
    for (const auto& pair: values){
        auto* value = payload.add_values();
        value->set_key(pair.first);
        value->set_value(pair.second->toString());
    }
    request.reads = {key, payload.SerializeAsString()};
    // sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::YCSB_DB::insert(const std::string &table, const std::string &key,
                                                 const utils::ByteIteratorMap &values) {
    return update(table, key, values);
}

ycsb::core::Status ycsb::client::YCSB_DB::remove(const std::string &table, const std::string &key) {
    Utils::Request request;
    request.funcName = "remove";
    request.tableName = table;
    request.reads = {key};
    // sendInvokeRequest(request);
    return core::STATUS_OK;
}