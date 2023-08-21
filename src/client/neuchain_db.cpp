//
// Created by user on 23-5-16.
//

#include "client/neuchain_db.h"
#include "client/neuchain_dbc.h"
#include "proto/user_connection.pb.h"
#include "common/timer.h"
#include <brpc/channel.h>
#include <thread>

namespace brpc::policy {
    DECLARE_int32(h2_client_connection_window_size);
}

namespace client {
    NeuChainDB::NeuChainDB(util::NodeConfigPtr server, std::shared_ptr<NeuChainDBConnection> dbc, std::shared_ptr<const util::Key> priKey) {
        if (brpc::policy::FLAGS_h2_client_connection_window_size < 1024 * 1024 * 100) {
            brpc::policy::FLAGS_h2_client_connection_window_size = 1024 * 1024 * 100;
        }
        _nextNonce = static_cast<int64_t>(utils::RandomUINT64::NewRandomUINT64()->nextValue() << 32);
        LOG(INFO) << "Created a connection to NeuChainDB with nonce: " << _nextNonce;
        CHECK(priKey->Private()) << "Can not sign using public key!";
        _invokeClient = std::move(dbc);
        _priKey = std::move(priKey);
        _serverConfig = std::move(server);
    }

    void NeuChainDB::stop() {
        _invokeClient->shutdown();
    }

    core::Status NeuChainDB::sendInvokeRequest(const std::string& chaincodeName, const std::string& funcName, const std::string& args) {
        // archive manually
        std::string data;
        zpp::bits::out out(data);
        UserRequestLikeStruct u {
                chaincodeName,
                funcName,
                args,
                _nextNonce,
        };
        _nextNonce += 1;
        if (failure(::proto::UserRequest::serialize(out, u))) {
            return core::ERROR;
        }
        // sign the envelope
        auto ret = _priKey->Sign(data.data(), data.size());
        if (ret == std::nullopt) {
            return core::ERROR;
        }
        EnvelopLikeStruct e;
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

    NeuChainStatus::NeuChainStatus(util::NodeConfigPtr server, int port, std::shared_ptr<const util::Key> priKey) {
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

    std::unique_ptr<::proto::Block> NeuChainStatus::getBlock(int blockNumber) {
        // We will receive response synchronously, safe to put variables on stack.
        proto::GetBlockRequest request;
        request.set_ski(_serverConfig->ski);
        request.set_chainid(_serverConfig->groupId);
        request.set_blockid(blockNumber);
        proto::GetBlockResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(5 * 1000);
        _stub->getBlock(&ctl, &request, &response, nullptr);
        if (ctl.Failed()) {
            // RMessage is too big
            LOG(ERROR) << "Failed to get block: " << blockNumber << ", Text: " << ctl.ErrorText() << ", Code: " << berror(ctl.ErrorCode());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
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

    bool NeuChainStatus::connect(int retryCount, int retryTimeoutMs) {
        // Send a request and wait for the response every retryTimeoutMs second.
        for (int i=0; i<retryCount; i++) {
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
            std::this_thread::sleep_for(std::chrono::milliseconds(retryTimeoutMs));
        }
        return false;
    }

    bool NeuChainStatus::getTop(int& blockNumber, int retryCount, int retryTimeoutMs) {
        // Get the current maximum block height
        for (int i=0; i<retryCount; i++) {
            // We will receive response synchronously, safe to put variables on stack.
            proto::GetTopRequest request;
            request.set_ski(_serverConfig->ski);
            request.set_chainid(_serverConfig->groupId);
            proto::GetTopResponse response;
            brpc::Controller ctl;
            _stub->getTop(&ctl, &request, &response, nullptr);
            if (!ctl.Failed() && response.success()) {
                DLOG(INFO) << "Start from block: " << response.blockid();
                blockNumber = response.blockid();
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(retryTimeoutMs));
        }
        return false;
    }

    std::unique_ptr<::proto::Block> NeuChainStatus::getLightBlock(int blockNumber, int64_t& timeMsWhenReturn) {
        proto::GetBlockRequest request;
        request.set_ski(_serverConfig->ski);
        request.set_chainid(_serverConfig->groupId);
        request.set_blockid(blockNumber);
        proto::GetBlockResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(5 * 1000);
        _stub->getLightBlock(&ctl, &request, &response, nullptr);
        timeMsWhenReturn = util::Timer::time_now_ms();
        if (ctl.Failed()) {
            // RMessage is too big
            LOG(ERROR) << "Failed to get block: " << blockNumber << ", Text: " << ctl.ErrorText() << ", Code: " << berror(ctl.ErrorCode());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return nullptr;
        }
        if (!response.success()) {
            LOG(ERROR) << "Failed to get block: " << blockNumber << ", " << response.payload();
            return nullptr;
        }
        auto block = std::make_unique<::proto::Block>();
        auto in = zpp::bits::in(response.payload());
        if(failure(in(block->header, block->body, block->executeResult.transactionFilter))) {
            return nullptr;
        }
        return block;
    }

    NeuChainStatus::~NeuChainStatus() = default;
}