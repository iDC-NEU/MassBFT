//
// Created by user on 23-5-16.
//

#include "ycsb/neuchain_db.h"
#include "proto/user_request.h"
#include "ycsb/core/workload/core_workload.h"
#include "common/property.h"

ycsb::core::Status
ycsb::client::NeuChainDB::read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) {
    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::scan(const std::string& table, const std::string& startkey, uint64_t recordcount, const std::vector<std::string>& fields) {
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

std::unique_ptr<proto::Block> ycsb::client::NeuChainDB::getBlock(int blockNumber) {
    rpcClient->send("block_query_" + std::to_string(blockNumber));
    auto reply = rpcClient->receive();
    if (reply == std::nullopt) {
        return nullptr;
    }
    auto block = std::make_unique<proto::Block>();
    auto ret = block->deserializeFromString(reply->to_string());
    if (!ret.valid) {
        return nullptr;
    }
    return block;
}

ycsb::core::Status ycsb::client::NeuChainDB::readModifyWrite(const std::string &table, const std::string &key,
                                                             const std::vector<std::string> &readFields,
                                                             const ycsb::utils::ByteIteratorMap &writeValues) {

    return core::STATUS_OK;
}
