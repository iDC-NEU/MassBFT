//
// Created by user on 23-7-12.
//

#include "ycsb/sdk/client_sdk.h"
#include "ycsb/neuchain_dbc.h"
#include "common/crypto.h"
#include "common/property.h"
#include "common/zmq_port_util.h"
#include "common/bccsp.h"
#include "common/yaml_key_storage.h"
#include "proto/user_connection.pb.h"
#include <brpc/channel.h>
#include <thread>

namespace ycsb::sdk {
    struct ClientSDKImpl {
        std::unique_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<const util::Key> _priKey;
        std::shared_ptr<util::NodeConfig> _targetLocalNode;
        int _sendPort = -1;
        int _receivePort = -1;
        std::shared_ptr<ycsb::client::NeuChainDBConnection> _dbc;
        std::unique_ptr<client::proto::UserService_Stub> _receiveStub;
        int64_t _nextNonce = 0;
    };

    void ClientSDK::InitSDKDependencies() {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
    }

    std::unique_ptr<ClientSDK> ClientSDK::NewSDK(const util::Properties &prop) {
        auto sdk = std::unique_ptr<ClientSDK>(new ClientSDK);
        // 1
        auto localNode = prop.getNodeProperties().getLocalNodeInfo();
        if (localNode == nullptr) {
            LOG(ERROR) << "Load local node info failed!";
            return nullptr;
        }
        // 2
        auto bcs = prop.getCustomPropertiesOrPanic("bccsp");
        auto bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(bcs));
        auto priKey = bccsp->GetKey(localNode->ski);
        if (!priKey || !priKey->Private()) {
            LOG(ERROR) << "Load private key failed!";
            return nullptr;
        }
        sdk->_impl->_bccsp = std::move(bccsp);
        sdk->_impl->_priKey = std::move(priKey);
        // 3
        auto portConfig = util::ZMQPortUtil::InitLocalPortsConfig(prop);
        sdk->_impl->_sendPort = portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[localNode->nodeId];
        sdk->_impl->_receivePort = portConfig->getLocalServicePorts(util::PortType::BFT_RPC)[localNode->nodeId];
        // 4
        sdk->_impl->_targetLocalNode = std::move(localNode);
        return sdk;
    }

    ClientSDK::ClientSDK() :_impl(new ClientSDKImpl) { }

    ClientSDK::~ClientSDK() = default;

    bool ClientSDK::connect() {
        const auto& server = _impl->_targetLocalNode;
        LOG(INFO) << "Connecting to peer: " << server->priIp << ":" << _impl->_receivePort;
        brpc::ChannelOptions options;
        options.protocol = "h2:grpc";
        options.timeout_ms = 200 /*milliseconds*/;
        options.max_retry = 0;
        auto channel = std::make_unique<brpc::Channel>();
        while(true) {
            if (channel->Init(server->priIp.data(), _impl->_receivePort, &options) == 0) {
                auto dbc = ycsb::client::NeuChainDBConnection::NewNeuChainDBConnection(server->priIp, _impl->_sendPort);
                if (dbc != nullptr) {
                    _impl->_dbc = std::move(dbc);
                    break;  //success
                }
            }
            LOG(WARNING) << "Try using public ip address:" << server->pubIp;
            channel = std::make_unique<brpc::Channel>();
            if (channel->Init(server->pubIp.data(), _impl->_receivePort, &options) == 0) {
                auto dbc = ycsb::client::NeuChainDBConnection::NewNeuChainDBConnection(server->pubIp, _impl->_sendPort);
                if (dbc != nullptr) {
                    _impl->_dbc = std::move(dbc);
                    break;  //success
                }
            }
            LOG(ERROR) << "Fail to initialize channel";
            return false;
        }
        _impl->_receiveStub = std::make_unique<client::proto::UserService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
        return true;
    }

    std::unique_ptr<proto::Envelop> ClientSDK::invokeChaincode(std::string ccName, std::string funcName, std::string args) const {
        // archive manually
        std::string data;
        zpp::bits::out out(data);
        proto::UserRequest u;
        u.setCCName(std::move(ccName));
        u.setFuncName(std::move(funcName));
        u.setArgs(std::move(args));

        if (failure(::proto::UserRequest::serialize(out, u))) {
            return nullptr;
        }
        proto::SignatureString signature;
        signature.nonce = _impl->_nextNonce++;
        if (failure(out(signature.nonce))) {
            return nullptr;
        }
        auto ret = _impl->_priKey->Sign(data.data(), data.size());
        if (ret == std::nullopt) {
            return nullptr;
        }
        signature.digest = *ret;
        signature.ski = _impl->_targetLocalNode->ski;

        auto envelop = std::make_unique<proto::Envelop>();
        envelop->setSignature(std::move(signature));
        envelop->setPayload(std::move(data));

        // serialize the envelop
        std::string dataEnvelop;
        zpp::bits::out outEnvelop(dataEnvelop);
        if (failure(outEnvelop(envelop))) {
            return nullptr;
        }
        if (!_impl->_dbc->send(std::move(dataEnvelop))) {
            return nullptr;
        }
        return envelop;
    }

    std::unique_ptr<proto::Block> ClientSDK::getBlock(int chainId, int blockId, int timeoutMs) const {
        // We will receive response synchronously, safe to put variables on stack.
        client::proto::PullBlockRequest request;
        request.set_ski(_impl->_targetLocalNode->ski);
        request.set_chainid(chainId);
        request.set_blockid(blockId);
        request.set_timeoutms(timeoutMs);
        client::proto::PullBlockResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(5 * 1000);
        _impl->_receiveStub->pullBlock(&ctl, &request, &response, nullptr);
        if (ctl.Failed()) {
            // RMessage is too big
            LOG(ERROR) << "Failed to get block: " << blockId << ", Text: " << ctl.ErrorText() << ", Code: " << berror(ctl.ErrorCode());
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            return nullptr;
        }
        if (!response.success()) {
            LOG(ERROR) << "Failed to get block: " << blockId << ", " << response.payload();
            return nullptr;
        }
        auto block = std::make_unique<::proto::Block>();
        auto ret = block->deserializeFromString(std::move(*response.mutable_payload()));
        if (!ret.valid) {
            LOG(ERROR) << "Failed to decode block: " << blockId;
            return nullptr;
        }
        return block;
    }

    int ClientSDK::getChainHeight(int chainId, int timeoutMs) const {
        // We will receive response synchronously, safe to put variables on stack.
        client::proto::GetTopRequest request;
        request.set_ski(_impl->_targetLocalNode->ski);
        request.set_chainid(chainId);
        client::proto::GetTopResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(timeoutMs);
        _impl->_receiveStub->getTop(&ctl, &request, &response, nullptr);
        if (!ctl.Failed() && response.success()) {
            return response.blockid();
        }
        return -1;
    }

    std::unique_ptr<BlockHeaderProof> ClientSDK::getBlockHeader(int chainId, int blockId, int timeoutMs) const {
        CHECK(false);
        return nullptr;
    }

    std::unique_ptr<TxMerkleProof> ClientSDK::getTxWithProof(const proto::DigestString &txId, int timeoutMs) const {
        CHECK(false);
        return nullptr;
    }

    std::unique_ptr<TxMerkleProof> ClientSDK::getTransaction(const proto::DigestString &txId, int timeoutMs) const {
        CHECK(false);
        return nullptr;
    }
}

