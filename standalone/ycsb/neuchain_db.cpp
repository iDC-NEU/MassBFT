//
// Created by user on 23-5-16.
//

#include <ycsb/neuchain_db.h>
#include <proto/user_request.h>
#include <ycsb/core/workload/core_workload.h>
#include "common/property.h"

ycsb::core::Status
ycsb::client::NeuChainDB::read(const std::string& table, const std::string& key,
                               const std::vector<std::string>& fields, utils::ByteIteratorMap& result) {
    proto::UserRequest request;
    request.setFuncName("read");
    YAML::Node n;
    ycsb::core::workload::CoreWorkload workload;
    workload.init(n);
    for(auto field: fields){

    }


    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::scan(const std::string& table, const std::string& startkey, uint64_t recordcount,
                                const std::vector<std::string>& fields, std::vector<utils::ByteIteratorMap>& result) {
    LOG(WARNING) << "Neuchain_db does not support Scan op. ";
    return core::ERROR;
}

ycsb::core::Status
ycsb::client::NeuChainDB::update(const std::string& table, const std::string& key,
                                 const utils::ByteIteratorMap& values) {

    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::insert(const std::string& table, const std::string& key,
                                 const utils::ByteIteratorMap& values) {
    update(table, key, values);
}

ycsb::core::Status
ycsb::client::NeuChainDB::remove(const std::string& table, const std::string& key) {
    proto::UserRequest request;
    request.setFuncName("del");

    return core::STATUS_OK;
}