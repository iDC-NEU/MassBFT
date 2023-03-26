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

        template<class ServerClass=util::MetaRpcServer>
        bool checkAndStart(std::unordered_map<int, ::util::NodeConfigPtr> localNodes
                , std::shared_ptr<util::BCCSP> bccsp
                , std::shared_ptr<PBFTStateMachine> stateMachine) {
            _localNodes = std::move(localNodes);
            _bccsp = std::move(bccsp);
            _stateMachine = std::move(stateMachine);
            if (_localNodes.empty() || !_bccsp || !_stateMachine) {
                LOG(ERROR) << "Fail to start globalControlService!";
                return false;
            }
            if (ServerClass::AddService(this, []{ }) != 0) {
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
            DLOG(INFO) << "requestProposal, Node: " << request->localid() << ", sequence: " << request->sequence();

            if (!_localNodes.contains(request->localid())) {
                LOG(WARNING) << "localId error.";
                return;
            }
            auto blockHeader = _stateMachine->OnRequestProposal(_localNodes.at(request->localid()), request->sequence(), request->payload());
            if (blockHeader == std::nullopt) {
                LOG(WARNING) << "get serialized block header error.";
                return;
            }
            response->set_payload(std::move(*blockHeader));
            response->set_success(true);
        }

        void verifyProposal(google::protobuf::RpcController*,
                            const proto::RPCRequest* request,
                            proto::RPCResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            DLOG(INFO) << "verifyProposal, Node: " << request->localid() << ", sequence: " << request->sequence();
            if (_lastVerifiedSequence >= request->sequence()) {
                LOG(WARNING) << "skip verified sequence.";
                response->set_success(true);
                return;
            }
            _lastVerifiedSequence = request->sequence();

            if (!_localNodes.contains(request->localid())) {
                LOG(WARNING) << "localId error.";
                return;
            }

            if (!_stateMachine->OnVerifyProposal(_localNodes.at(request->localid()), request->payload())) {
                LOG(WARNING) << "Validate block error.";
                return;
            }
            // DLOG(INFO) << "verifyProposal finished, Node: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }

        void signProposal(google::protobuf::RpcController*,
                          const proto::RPCRequest* request,
                          proto::RPCResponse* response,
                          ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            DLOG(INFO) << "signProposal, Node: " << request->localid() << ", sequence: " << request->sequence();
            auto nodeId = request->localid();
            if (!_localNodes.contains(nodeId)) {
                LOG(WARNING) << "Wrong node id.";
                return;
            }
            auto ret = _stateMachine->OnSignMessage(_localNodes.at(request->localid()), request->payload());
            if (ret == std::nullopt) {
                response->set_success(false);
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
            DLOG(INFO) << "deliver, Node: " << request->localid() << ", sequence: " << request->sequence();
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

            std::vector<::proto::SignatureString> consensusSignatures(request->contents_size());
            // append signatures
            for (int i=0; i<request->contents_size(); i++) {
                auto& it = consensusSignatures[i];
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
            if (!_stateMachine->OnDeliver(_localNodes.at(request->localid()), tomMessage.content(), std::move(consensusSignatures))) {
                LOG(WARNING) << "Validate block error.";
                return;
            }
            // DLOG(INFO) << "deliver finished, Node: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }

        void leaderStart(google::protobuf::RpcController*,
                         const proto::RPCRequest* request,
                         proto::RPCResponse* response,
                         ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            DLOG(INFO) << "leaderStart, Node: " << request->localid() << ", sequence: " << request->sequence();
            if (!_localNodes.contains(request->localid())) {
                LOG(WARNING) << "localId error.";
                return;
            }
            _stateMachine->OnLeaderStart(_localNodes.at(request->localid()), request->sequence());
            response->set_success(true);
        }

        void leaderStop(google::protobuf::RpcController*,
                        const proto::RPCRequest* request,
                        proto::RPCResponse* response,
                        ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            DLOG(INFO) << "leaderStop, Node: " << request->localid() << ", sequence: " << request->sequence();
            if (!_localNodes.contains(request->localid())) {
                LOG(WARNING) << "localId error.";
                return;
            }
            _stateMachine->OnLeaderStop(_localNodes.at(request->localid()), request->sequence());
            response->set_success(true);
        }

    private:
        // key: node id, value: node ski
        std::unordered_map<int, ::util::NodeConfigPtr> _localNodes;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<PBFTStateMachine> _stateMachine;
        int _lastVerifiedSequence = -1;
    };
}