//
// Created by user on 23-7-12.
//

#include "client/sdk/client_sdk.h"
#include "client/neuchain_dbc.h"
#include "client/neuchain_db.h"
#include "common/crypto.h"
#include "common/property.h"
#include "common/proof_generator.h"
#include "common/zmq_port_util.h"
#include "common/bccsp.h"
#include "common/yaml_key_storage.h"
#include "proto/user_connection.pb.h"
#include <brpc/channel.h>
#include <thread>

namespace client::sdk {
    struct ClientSDKImpl {
        std::unique_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::NodeConfig> _targetLocalNode;
        int _sendPort = -1;
        int _receivePort = -1;
        std::unique_ptr<client::NeuChainDB> _db;
        std::unique_ptr<::client::proto::UserService_Stub> _receiveStub;
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
        sdk->_impl->_bccsp = std::move(bccsp);
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
        auto initNeuChainDB = [&](auto&& dbc) {
            auto priKey = _impl->_bccsp->GetKey(_impl->_targetLocalNode->ski);
            if (!priKey || !priKey->Private()) {
                LOG(ERROR) << "Load private key failed!";
                return false;
            }
            _impl->_db = std::make_unique<client::NeuChainDB>(_impl->_targetLocalNode, std::forward<decltype(dbc)>(dbc), std::move(priKey));
            return true;
        };
        while(true) {
            if (channel->Init(server->priIp.data(), _impl->_receivePort, &options) == 0) {
                auto dbc = ::client::NeuChainDBConnection::NewNeuChainDBConnection(server->priIp, _impl->_sendPort);
                if (dbc != nullptr) {
                    initNeuChainDB(std::move(dbc));
                    break;  //success
                }
            }
            LOG(WARNING) << "Try using public ip address:" << server->pubIp;
            channel = std::make_unique<brpc::Channel>();
            if (channel->Init(server->pubIp.data(), _impl->_receivePort, &options) == 0) {
                auto dbc = ::client::NeuChainDBConnection::NewNeuChainDBConnection(server->pubIp, _impl->_sendPort);
                if (dbc != nullptr) {
                    initNeuChainDB(std::move(dbc));
                    break;  //success
                }
            }
            LOG(ERROR) << "Fail to initialize channel";
            return false;
        }
        _impl->_receiveStub = std::make_unique<client::proto::UserService_Stub>(channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);
        return true;
    }

    core::Status ClientSDK::invokeChaincode(std::string ccName, std::string funcName, std::string args) const {
        return static_cast<core::DB*>(_impl->_db.get())->sendInvokeRequest(ccName, funcName, args);
    }

    std::unique_ptr<::proto::Block> ClientSDK::getBlock(int chainId, int blockId, int timeoutMs) const {
        // We will receive response synchronously, safe to put variables on stack.
        client::proto::GetBlockRequest request;
        request.set_ski(_impl->_targetLocalNode->ski);
        request.set_chainid(chainId);
        request.set_blockid(blockId);
        request.set_timeoutms(timeoutMs);
        client::proto::GetBlockResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(5 * 1000);
        _impl->_receiveStub->getBlock(&ctl, &request, &response, nullptr);
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
        client::proto::GetBlockHeaderRequest request;
        request.set_chainid(chainId);
        request.set_blockid(blockId);
        client::proto::GetBlockHeaderResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(timeoutMs);
        _impl->_receiveStub->getBlockHeader(&ctl, &request, &response, nullptr);
        if (ctl.Failed() || !response.success()) {
            return nullptr;
        }
        auto headerWithProof = std::make_unique<BlockHeaderProof>();
        zpp::bits::in hIn(response.header());
        if (failure(hIn(headerWithProof->header))) {
            return nullptr;
        }
        zpp::bits::in mIn(response.metadata());
        if (failure(mIn(headerWithProof->metadata))) {
            return nullptr;
        }
        return headerWithProof;
    }

    std::unique_ptr<TxMerkleProof> ClientSDK::getTransaction(const ::proto::DigestString &txId,
                                                             int chainIdHint,
                                                             int blockIdHint,
                                                             int timeoutMs) const {
        client::proto::GetTxRequest request;
        request.set_chainidhint(chainIdHint);
        request.set_blockidhint(blockIdHint);
        request.set_txid(txId.data(), txId.size());
        request.set_timeoutms(timeoutMs);
        client::proto::GetTxResponse response;
        brpc::Controller ctl;
        ctl.set_timeout_ms(timeoutMs);
        _impl->_receiveStub->getTxWithProof(&ctl, &request, &response, nullptr);
        if (ctl.Failed()) {
            return nullptr;
        }
        if (!response.success()) {
            LOG(ERROR) << "Failed to get transaction.";
            return nullptr;
        }
        // --- set envelop
        auto respWithProof = std::make_unique<TxMerkleProof>();
        if (!deserializeFromString(response.envelopproof(), respWithProof->envelopProof)) {
            return nullptr;
        }
        auto envelop = std::make_unique<::proto::Envelop>();
        if (envelop->deserializeFromString(response.envelop()) < 0) {
            return nullptr;
        }
        respWithProof->envelop = std::move(envelop);
        // ---set rw set.
        if (response.has_rwsetproof() && !deserializeFromString(response.rwsetproof(), respWithProof->rwSetProof)) {
            return nullptr;
        }
        if (response.has_rwset()) {
            auto rwSet = std::make_unique<::proto::TxReadWriteSet>();
            zpp::bits::in in(response.rwset());
            if (failure(in(*rwSet, respWithProof->valid))) {
                return nullptr;
            }
            respWithProof->rwSet = std::move(rwSet);
        }
        return respWithProof;
    }

    bool deserializeFromString(const std::string &raw, ProofLikeStruct &ret, int startPos) {
        zpp::bits::in in(raw);
        in.reset(startPos);
        int64_t proofSize;
        if (failure(in(ret.Path, proofSize))) {
            return false;
        }
        ret.Siblings.resize(proofSize);
        for (int i=0; i<(int)proofSize; i++) {
            if(failure(in(ret.Siblings[i]))) {
                return false;
            }
        }
        return true;
    }

    bool ReceiveInterface::ValidateUserRequestMerkleProof(const ::proto::HashString &root,
                                                          const ProofLikeStruct &proof,
                                                          const ::proto::Envelop &envelop) {
        pmt::Proof proofView;
        for (const auto& it: proof.Siblings) {
            proofView.Siblings.push_back(&it);
        }
        proofView.Path = proof.Path;
        return util::UserRequestMTGenerator::ValidateProof(root, proofView, envelop);
    }

    bool ReceiveInterface::ValidateExecResultMerkleProof(const ::proto::HashString &root,
                                                         const ProofLikeStruct &proof,
                                                         const ::proto::TxReadWriteSet &rwSet,
                                                         std::byte filter) {
        pmt::Proof proofView;
        for (const auto& it: proof.Siblings) {
            proofView.Siblings.push_back(&it);
        }
        proofView.Path = proof.Path;
        return util::ExecResultMTGenerator::ValidateProof(root, proofView, rwSet, filter);
    }
}
