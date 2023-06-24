//
// Created by user on 23-5-16.
//

#include "ycsb/neuchain_db.h"
#include "proto/user_request.h"
#include "ycsb/core/workload/core_workload.h"
#include "common/property.h"
#include "ycsb/core/request_sender.h"

ycsb::core::Status
ycsb::client::NeuChainDB::db_connection() {
    auto property = utils::YCSBProperties::NewFromProperty();
    rpcClient.emplace_back(property->getLocalBlockServerIP(),
                             util::ZMQInstance::NewClient<zmq::socket_type::push>(property->getLocalBlockServerIP(), 51200));
    if(property->sendToAllClientProxy()) {
        // load average
        for(const auto& ip: property->getBlockServerIPs()) {
            invokeClient.emplace_back(ip, util::ZMQInstance::NewClient<zmq::socket_type::push>(ip, 51200));
            // queryClient.emplace_back(ip, std::make_unique<ZMQClient>(ip, "7003"));
        }
        sendInvokeRequest = [this](proto::UserRequest &request) {
            size_t clientCount = invokeClient.size();
            size_t id = trCount % clientCount;

            std::string requestRaw;
            zpp::bits::out out(requestRaw);
            CHECK(!failure(out(request)));
            // wrap it
            std::unique_ptr<proto::Envelop> envelop(new proto::Envelop);
            envelop->setPayload(std::move(requestRaw));
            // compute tid

            // TODO：
            proto::SignatureString sig = {"ski", std::make_shared<std::string>(), {}};
            auto digest = std::to_string(id);
            std::copy(digest.begin(), digest.end(), sig.digest.data());
            envelop->setSignature(std::move(sig));
            // addPendingTransactionHandle(invokeRequest.digest());
            std::string envelopRaw;
            envelop->serializeToString(&envelopRaw);
            invokeClient[id].second->send(std::move(envelopRaw));
        };
    } else {
        // single user
        const auto& ip = property->getLocalBlockServerIP();
        invokeClient.emplace_back(ip, util::ZMQInstance::NewClient<zmq::socket_type::push>(ip, 51200));
        sendInvokeRequest = [this](proto::UserRequest &request) {
            std::string requestRaw;
            zpp::bits::out out(requestRaw);
            CHECK(!failure(out(request)));
            // wrap it
            std::unique_ptr<proto::Envelop> envelop(new proto::Envelop);
            envelop->setPayload(std::move(requestRaw));
            // compute tid
            proto::SignatureString signature;
            auto digest = std::to_string(0);
            std::copy(digest.begin(), digest.end(), signature.digest.data());
            envelop->setSignature(std::move(signature));
            // addPendingTransactionHandle(invokeRequest.digest());
            std::string envelopRaw;
            envelop->serializeToString(&envelopRaw);
            invokeClient[0].second->send(std::move(envelopRaw));
        };
    }
    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) {
    proto::UserRequest request;
    request.setFuncName("read");
    request.setCCName("ycsb");

    // args = key + 0(read) + fields(field + "-")
    std::string args;
    std::string separator = " ";
    args.append(key).append(separator);
    args.append(std::to_string(static_cast<double>(DBOperation::READ)));
    for(const auto& pair: fields){
        args.append(pair + separator +  "-");
    }
    request.setArgs(std::move(args));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::scan(const std::string& table, const std::string& startkey, uint64_t recordcount, const std::vector<std::string>& fields) {
    LOG(WARNING) << "neuChain_db does not support Scan op. ";
    return core::ERROR;
}

ycsb::core::Status
ycsb::client::NeuChainDB::update(const std::string& table, const std::string& key,
                                 const utils::ByteIteratorMap& values) {
    proto::UserRequest request;
    request.setFuncName("write");
    request.setCCName("ycsb");

    // args = key + 1(write) + values(field + value)
    std::string args;
    std::string separator = " ";
    args.append(key).append(separator);
    args.append(std::to_string(static_cast<double>(DBOperation::WRITE)));
    for(const auto& pair: values){
        args.append(pair.first + separator +  pair.second);
    }
    request.setArgs(std::move(args));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::insert(const std::string& table, const std::string& key,
                                 const utils::ByteIteratorMap& values) {
    update(table, key, values);
    return core::STATUS_OK;
}

ycsb::core::Status
ycsb::client::NeuChainDB::remove(const std::string& table, const std::string& key) {
    proto::UserRequest request;
    request.setFuncName("del");
    request.setCCName("ycsb");
    request.setArgs(const_cast<std::string &&>(key));
    sendInvokeRequest(request);
    return core::STATUS_OK;
}

std::unique_ptr<proto::Block> ycsb::client::NeuChainDB::getBlock(int blockNumber) {
    rpcClient[0].second->send("block_query_" + std::to_string(blockNumber));
    auto reply = rpcClient[0].second->receive();
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

ycsb::core::Status
ycsb::client::NeuChainDB::readModifyWrite(const std::string &table, const std::string &key,
                                          const std::vector<std::string> &readFields,
                                          const ycsb::utils::ByteIteratorMap &writeValues) {

    return core::STATUS_OK;
}
