//
// Created by user on 23-3-21.
//

#pragma once

#include "peer/storage/mr_block_storage.h"
#include "peer/consensus/pbft/pbft_state_machine.h"

#include "common/meta_rpc_server.h"
#include "common/bccsp.h"
#include "common/property.h"

#include "proto/pbft_connection.pb.h"
#include "proto/pbft_message.pb.h"

namespace peer::consensus {

    class PBFTRPCService : public proto::RPCService {
    public:
        PBFTRPCService() = default;

        PBFTRPCService(const PBFTRPCService&) = delete;

        PBFTRPCService(PBFTRPCService&&) = delete;

        bool checkAndStart(std::unordered_map<int, ::util::NodeConfigPtr> localNodes
                , std::shared_ptr<util::BCCSP> bccsp
                , std::shared_ptr<PBFTStateMachine> stateMachine
                , std::shared_ptr<::proto::Block> lastBlock=nullptr) {
            _localNodes = std::move(localNodes);
            _bccsp = std::move(bccsp);
            _stateMachine = std::move(stateMachine);
            _lastBlock = std::move(lastBlock);
            if (_localNodes.empty() || !_bccsp || !_stateMachine) {
                LOG(ERROR) << "Fail to start globalControlService!";
                return false;
            }
            if (util::MetaRpcServer::AddService(this, []{ }) != 0) {
                LOG(ERROR) << "Fail to add globalControlService!";
                return false;
            }
            return true;
        }

    protected:
        void requestProposal(google::protobuf::RpcController*,
                             const proto::RPCRequest* request,
                             proto::RPCResponse* response,
                             ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            const auto regionId = request->localid();
            int nextBlockNumber = 0;
            if (_lastBlock != nullptr) {
                nextBlockNumber = (int)_lastBlock->header.number;
            }
            auto block = _stateMachine->OnRequestProposal(regionId, nextBlockNumber, request->payload());
            if (block == nullptr) {
                LOG(INFO) << "Get Block failed, number: " << nextBlockNumber << ", region: " << regionId;
                return;
            }
            if (_lastBlock != nullptr) {
                DCHECK(_lastBlock->header.dataHash == block->header.previousHash);
                DCHECK(_lastBlock->header.number + 1 == block->header.number);
            }
            if (block->haveSerializedMessage()) {
                LOG(WARNING) << "Using cached message, please ensure the message is newest.";
                response->set_payload(*block->getSerializedMessage());
            } else {
                std::string serializedMessage;
                block->serializeToString(&serializedMessage);
                response->set_payload(std::move(serializedMessage));
            }
            _lastBlock = std::move(block);
            response->set_success(true);
        }

        void verifyProposal(google::protobuf::RpcController*,
                            const proto::RPCRequest* request,
                            proto::RPCResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);

            auto block = std::make_unique<::proto::Block>();
            auto ret = block->deserializeFromString(std::string(request->payload()));
            if (!ret.valid) {
                LOG(WARNING) << "Deserialize block error.";
                return;
            }
            if (!_stateMachine->OnVerifyProposal(std::move(block))) {
                LOG(WARNING) << "Validate block error.";
                return;
            }
            response->set_success(true);
        }

        void signProposal(google::protobuf::RpcController*,
                          const proto::RPCRequest* request,
                          proto::RPCResponse* response,
                          ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            auto nodeId = request->localid();
            if (!_localNodes.contains(nodeId)) {
                LOG(WARNING) << "Wrong node id.";
                return;
            }
            // Get the private key of this node
            const auto key = _bccsp->GetKey(_localNodes[nodeId]->ski);
            if (key == nullptr || !key->Private()) {
                LOG(WARNING) << "Can not load key.";
                return;
            }
            // sign the message with the private key
            auto ret = key->Sign(request->payload().data(), request->payload().size());
            if (ret == std::nullopt) {
                LOG(WARNING) << "Sign data failed.";
                return;
            }
            response->set_payload(ret->data(), ret->size());
            response->set_success(true);
        }

        void deliver(google::protobuf::RpcController*,
                     const proto::DeliverRequest* request,
                     proto::RPCResponse* response,
                     ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            // unwrap the content
            proto::Proposal proposal;
            if(!proposal.ParseFromString(request->proposevalue())) {
                LOG(WARNING) << "Deserialize proposal failed!";
                return;
            }
            proto::TOMMessage tomMessage;
            if(!tomMessage.ParseFromString(proposal.message())) {
                LOG(WARNING) << "Deserialize tomMessage failed!";
                return;
            }

            auto block = std::make_unique<::proto::Block>();
            auto ret = block->deserializeFromString(std::string(tomMessage.content()));
            if (!ret.valid) {
                LOG(WARNING) << "Deserialize block error.";
                return;
            }
            // append signatures
            block->metadata.consensusSignatures.resize(request->contents_size());
            for (int i=0; i<request->contents_size(); i++) {
                auto& it = block->metadata.consensusSignatures[i];
                std::memcpy(it.digest.data(), request->signatures(i).data(), it.digest.size());
                if (!_localNodes.contains(request->localid())) {
                    LOG(WARNING) << "Signatures contains error.";
                    return;
                }
                it.ski = _localNodes.at(request->localid())->ski;
                // After consensus, the consensus service may not only sign the hash of the block,
                // we need to keep the content corresponding to the signature for verification
                it.content = std::make_unique<std::string>( request->contents(i));
            }
            if (!_stateMachine->OnDeliver(std::move(block))) {
                LOG(WARNING) << "Validate block error.";
                return;
            }
            response->set_success(true);
        }

        void leaderStart(google::protobuf::RpcController*,
                         const proto::RPCRequest* request,
                         proto::RPCResponse* response,
                         ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            _stateMachine->OnLeaderStart(request->payload());
            response->set_success(true);
        }

        void leaderStop(google::protobuf::RpcController*,
                        const proto::RPCRequest* request,
                        proto::RPCResponse* response,
                        ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            _stateMachine->OnLeaderStop(request->payload());
            response->set_success(true);
        }

    private:
        // key: node id, value: node ski
        std::unordered_map<int, ::util::NodeConfigPtr> _localNodes;
        std::shared_ptr<::proto::Block> _lastBlock;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<PBFTStateMachine> _stateMachine;
    };
}