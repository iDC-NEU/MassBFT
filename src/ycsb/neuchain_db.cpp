//
// Created by user on 23-5-16.
//

#include "ycsb/neuchain_db.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/common/random_uint64.h"
#include "proto/user_request.h"
#include "proto/user_connection.pb.h"
#include <brpc/channel.h>

namespace brpc::policy {
    DECLARE_int32(h2_client_connection_window_size);
}

ycsb::client::NeuChainDB::NeuChainDB(util::NodeConfigPtr server, std::shared_ptr<NeuChainDBConnection> dbc, std::shared_ptr<const util::Key> priKey) {
    if (brpc::policy::FLAGS_h2_client_connection_window_size < 1024 * 1024 * 10) {
        brpc::policy::FLAGS_h2_client_connection_window_size = 1024 * 1024 * 10;
    }
    _nextNonce = static_cast<int64_t>(::ycsb::utils::RandomUINT64::NewRandomUINT64()->nextValue() << 32);
    LOG(INFO) << "Created a connection to NeuChainDB with nonce: " << _nextNonce;
    CHECK(priKey->Private()) << "Can not sign using public key!";
    _invokeClient = std::move(dbc);
    _priKey = std::move(priKey);
    _serverConfig = std::move(server);
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
    e._payloadSV = data;
    e._signature.digest = *ret;
    e._signature.ski = _serverConfig->ski;
    // serialize the data
    std::string dataEnvelop;
    zpp::bits::out outEnvelop(dataEnvelop);
    if (failure(::proto::Envelop::serialize(outEnvelop, e))) {
        return core::ERROR;
    }
    auto timeNowMs = util::Timer::time_now_ms();
    if (!_invokeClient->send(std::move(dataEnvelop))) {
        return core::ERROR;
    }
    return core::Status(core::Status::State::OK,
                        timeNowMs,
                        std::string(reinterpret_cast<const char *>(e._signature.digest.data()), e._signature.digest.size()));
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
    // We will receive response synchronously, safe to put variables on stack.
    proto::PullBlockRequest request;
    request.set_ski(_serverConfig->ski);
    request.set_chainid(_serverConfig->groupId);
    request.set_blockid(blockNumber);
    proto::PullBlockResponse response;
    brpc::Controller ctl;
    ctl.set_timeout_ms(5 * 1000);
    _stub->pullBlock(&ctl, &request, &response, nullptr);
    if (ctl.Failed()) {
        // RMessage is too big
        LOG(ERROR) << "Failed to get block: " << blockNumber
                   << ", Text: " << ctl.ErrorText()
                   << ", Code: " << berror(ctl.ErrorCode());
        return nullptr;
    }
    if (!response.success()) {
        LOG(ERROR) << "Failed to get block: " << blockNumber << ", " << response.payload();
        return nullptr;
    }
    auto block = std::make_unique<::proto::Block>();
    auto ret = block->deserializeFromString(std::move(*response.mutable_payload()));
    if (!ret.valid) {
        LOG(ERROR) << "Failed to decode block: " << blockNumber;
        return nullptr;
    }
    return block;
}

bool ycsb::client::NeuChainStatus::connect() {
    // Send a request and wait for the response every 1 second.
    for (int i=0; i<100; i++) {
        // We will receive response synchronously, safe to put variables on stack.
        proto::HelloRequest request;
        request.set_ski(_serverConfig->ski);
        proto::HelloResponse response;
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
