//
// Created by user on 23-5-16.
//

#include "ycsb/neuchain_db.h"
#include "ycsb/core/workload/core_workload.h"
#include "proto/user_request.h"
#include "common/property.h"

ycsb::core::Status ycsb::client::NeuChainDB::read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) {
    proto::UserRequest request;
    request.setFuncName("read");
    request.setCCName("ycsb");

    // args = key + 0(read) + fields(field + "-")
    std::string args;
    std::string separator = " ";
    args.append(key).append(separator);
    args.append(std::to_string(static_cast<double>(Op::READ)));
    for(const auto& pair: fields){
        args.append(pair + separator +  "-");
    }
    request.setArgs(std::move(args));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::NeuChainDB::scan(const std::string& table, const std::string& startKey, uint64_t recordCount, const std::vector<std::string>& fields) {
    LOG(WARNING) << "neuChain_db does not support Scan op. ";
    return core::ERROR;
}

ycsb::core::Status ycsb::client::NeuChainDB::update(const std::string& table, const std::string& key,
                                                    const utils::ByteIteratorMap& values) {
    proto::UserRequest request;
    request.setFuncName("write");
    request.setCCName("ycsb");

    // args = key + 1(write) + values(field + value)
    std::string args;
    std::string separator = " ";
    args.append(key).append(separator);
    args.append(std::to_string(static_cast<double>(Op::WRITE)));
    for(const auto& pair: values){
        args.append(pair.first + separator +  pair.second);
    }
    request.setArgs(std::move(args));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::NeuChainDB::insert(const std::string& table, const std::string& key,
                                                    const utils::ByteIteratorMap& values) {
    update(table, key, values);
    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::NeuChainDB::remove(const std::string& table, const std::string& key) {
    proto::UserRequest request;
    request.setFuncName("del");
    request.setCCName("ycsb");
    request.setArgs(const_cast<std::string &&>(key));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status ycsb::client::NeuChainDB::readModifyWrite(const std::string &table, const std::string &key,
                                                             const std::vector<std::string> &readFields,
                                                             const ycsb::utils::ByteIteratorMap &writeValues) {

    return core::STATUS_OK;
}

ycsb::client::NeuChainDB::NeuChainDB() {
    LOG(INFO) << "Created a connection to NeuChainDB.";
    auto* prop = util::Properties::GetProperties();
    auto localNode = prop->getNodeProperties().getLocalNodeInfo();
    // TODO: set port
    invokeClient = util::ZMQInstance::NewClient<zmq::socket_type::push>(localNode->priIp, 51200);
}

bool ycsb::client::NeuChainDB::sendInvokeRequest(const proto::UserRequest &request) {
    std::string requestRaw;
    zpp::bits::out out(requestRaw);
    if(failure(out(request))) {
        return false;
    }
    // wrap it
    proto::Envelop envelop;
    envelop.setPayload(std::move(requestRaw));
    // compute tid
    proto::SignatureString signature;
    // TODO: set digest
    auto digest = std::to_string(0);
    std::copy(digest.begin(), digest.end(), signature.digest.data());
    envelop.setSignature(std::move(signature));
    std::string envelopRaw;
    if (!envelop.serializeToString(&envelopRaw)) {
        return false;
    }
    return invokeClient->send(std::move(envelopRaw));
}

std::unique_ptr<proto::Block> ycsb::client::NeuChainStatus::getBlock(int blockNumber) {
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

ycsb::client::NeuChainStatus::NeuChainStatus() {
    LOG(INFO) << "Created a connection to NeuChainStatus.";
    auto* prop = util::Properties::GetProperties();
    auto localNode = prop->getNodeProperties().getLocalNodeInfo();
    // TODO: set port
    rpcClient = util::ZMQInstance::NewClient<zmq::socket_type::push>(localNode->priIp, 51200);
}
