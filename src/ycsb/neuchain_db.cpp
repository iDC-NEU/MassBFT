//
// Created by user on 23-5-16.
//

#include "ycsb/neuchain_db.h"
#include "ycsb/core/workload/core_workload.h"
#include "proto/user_request.h"
#include "proto/user_connection.pb.h"
#include <brpc/channel.h>

ycsb::client::NeuChainDB::NeuChainDB(util::NodeConfigPtr server, int port, std::shared_ptr<const util::Key> priKey) {
    LOG(INFO) << "Created a connection to NeuChainDB.";
    CHECK(priKey->Private()) << "Can not sign using public key!";
    _invokeClient = util::ZMQInstance::NewClient<zmq::socket_type::pub>(server->priIp, port);
    _priKey = std::move(priKey);
    _serverConfig = std::move(server);
    _nextNonce = static_cast<int64_t>(std::hash<std::thread::id>{}(std::this_thread::get_id()) << 32);
}

void ycsb::client::NeuChainDB::stop() {
    _invokeClient->shutdown();
}

ycsb::core::Status ycsb::client::NeuChainDB::read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) {
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
    return sendInvokeRequest(InvokeRequestType::READ, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::scan(const std::string &table,
                                                  const std::string &startKey,
                                                  uint64_t recordCount,
                                                  const std::vector<std::string> &fields) {
    std::string data;
    zpp::bits::out out(data);
    if (failure(out(table, startKey, recordCount, fields))) {
        return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::SCAN, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) {
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
    return sendInvokeRequest(InvokeRequestType::UPDATE, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::readModifyWrite(const std::string &table, const std::string &key,
                                                             const std::vector<std::string> &readFields,
                                                             const ycsb::utils::ByteIteratorMap &writeValues) {
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
    return sendInvokeRequest(InvokeRequestType::READ_MODIFY_WRITE, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::insert(const std::string &table, const std::string &key, const ycsb::utils::ByteIteratorMap &values) {
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
    return sendInvokeRequest(InvokeRequestType::INSERT, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::remove(const std::string &table, const std::string &key) {
    std::string data;
    zpp::bits::out out(data);
    // only table and key
    if (failure(out(table, key))) {
        return core::ERROR;
    }
    return sendInvokeRequest(InvokeRequestType::DELETE, data);
}

ycsb::core::Status ycsb::client::NeuChainDB::sendInvokeRequest(const std::string& funcName, const std::string& args) {
    // archive manually
    std::string data;
    zpp::bits::out out(data);
    UserRequestLikeStruct u {
        InvokeRequestType::YCSB,
        funcName,
        args
    };
    if (failure(::proto::UserRequest::serialize(out, u))) {
        return core::ERROR;
    }
    EnvelopLikeStruct e;
    e._payloadSV = data;
    // append the nonce
    e._signature.nonce = _nextNonce;
    if (failure(out(e._signature.nonce))) {
        return core::ERROR;
    }
    _nextNonce += 1;
    // sign the envelope
    auto ret = _priKey->Sign(data.data(), data.size());
    if (ret == std::nullopt) {
        return core::ERROR;
    }
    e._signature.digest = *ret;
    e._signature.ski = _serverConfig->ski;
    // serialize the data
    std::string dataEnvelop;
    zpp::bits::out outEnvelop(dataEnvelop);
    if (failure(::proto::Envelop::serialize(outEnvelop, e))) {
        return core::ERROR;
    }
    if (!_invokeClient->send(std::move(dataEnvelop))) {
        return core::ERROR;
    }
    return core::Status(core::Status::State::OK, std::string(reinterpret_cast<const char *>(e._signature.digest.data()), e._signature.digest.size()));
}

ycsb::client::NeuChainStatus::NeuChainStatus(util::NodeConfigPtr server, int port, std::shared_ptr<const util::Key> priKey) {
    LOG(INFO) << "Created a connection to NeuChainStatus.";
    CHECK(priKey->Private()) << "Can not sign using public key!";
    // A Channel represents a communication line to a Server. Notice that
    // Channel is thread-safe and can be shared by all threads in your program.
    // Initialize the channel, NULL means using default options.
    brpc::ChannelOptions options;
    options.protocol = "h2:grpc";
    // options.timeout_ms = 200 /*milliseconds*/;
    options.max_retry = 0;
    auto channel = std::make_unique<brpc::Channel>();
    if (channel->Init(server->priIp.data(), port, &options) != 0) {
        CHECK(false) << "Fail to initialize channel";
    }
    // Normally, you should not call a Channel directly, but instead construct
    // a stub Service wrapping it. stub can be shared by all threads as well.
    _stub = std::make_unique<proto::UserService_Stub>(channel.get());
    _priKey = std::move(priKey);
    _serverConfig = std::move(server);
    _channel = std::move(channel);
}

std::unique_ptr<proto::Block> ycsb::client::NeuChainStatus::getBlock(int blockNumber) {
    // Send a request and wait for the response every 1 second.
    for (int i=0; i<5; i++) {
        // We will receive response synchronously, safe to put variables on stack.
        proto::UserRequest request;
        *request.mutable_payload() = std::to_string(blockNumber);
        proto::UserResponse response;
        brpc::Controller ctl;
        _stub->pullBlock(&ctl, &request, &response, nullptr);
        if (!ctl.Failed() && response.success()) {
            auto block = std::make_unique<::proto::Block>();
            auto ret = block->deserializeFromString(std::move(*response.mutable_payload()));
            if (ret.valid) {
                return block;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(ERROR) << "Failed to get block: " << blockNumber;
    return nullptr;
}

bool ycsb::client::NeuChainStatus::connect() {
    // Send a request and wait for the response every 1 second.
    for (int i=0; i<100; i++) {
        // We will receive response synchronously, safe to put variables on stack.
        proto::UserRequest request;
        proto::UserResponse response;
        brpc::Controller ctl;
        _stub->hello(&ctl, &request, &response, nullptr);
        if (!ctl.Failed() && response.success()) {
            DLOG(INFO) << "Connected: " << response.payload();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return false;
}

ycsb::client::NeuChainStatus::~NeuChainStatus() = default;
