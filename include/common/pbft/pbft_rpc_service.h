//
// Created by user on 23-3-21.
//

#pragma once

#include "peer/storage/mr_block_storage.h"
#include "common/pbft/pbft_state_machine.h"

#include "common/meta_rpc_server.h"
#include "common/bccsp.h"
#include "common/property.h"

#include "proto/pbft_connection.pb.h"
#include "proto/pbft_message.pb.h"

namespace util::pbft {

    class PBFTRPCService : public proto::RPCService {
    public:
        PBFTRPCService() = default;

        PBFTRPCService(const PBFTRPCService&) = delete;

        PBFTRPCService(PBFTRPCService&&) = delete;

        bool checkAndStart(std::vector<std::shared_ptr<util::NodeConfig>> localNodes
                , std::shared_ptr<util::BCCSP> bccsp
                , std::shared_ptr<PBFTStateMachine> stateMachine) {
            _localNodes = std::move(localNodes);
            _bccsp = std::move(bccsp);
            _stateMachine = std::move(stateMachine);
            if (_localNodes.empty() || !_bccsp || !_stateMachine) {
                LOG(ERROR) << "Fail to start globalControlService!";
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
            DLOG(INFO) << "requestProposal, sequence: " << request->sequence()  << ", threadId: " << std::this_thread::get_id();

            if ((int)_localNodes.size() <= request->localid()) {
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
            // DLOG(INFO) << "verifyProposal, Node: " << request->localid() << ", sequence: " << request->sequence();

            if ((int)_localNodes.size() <= request->localid()) {
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
            // DLOG(INFO) << "signProposal, Node: " << request->localid() << ", sequence: " << request->sequence();
            auto nodeId = request->localid();
            if ((int)_localNodes.size() <= nodeId) {
                LOG(WARNING) << "Wrong node id.";
                return;
            }
            auto signature = _stateMachine->OnSignProposal(_localNodes.at(request->localid()), request->payload());
            if (signature == nullptr) {
                return;
            }
            auto* payload = response->mutable_payload();
            payload->reserve(1024);
            zpp::bits::out out(*payload);
            if (failure(out(*signature))) {
                return;
            }
            response->set_success(true);
        }

        void deliver(google::protobuf::RpcController*,
                     const proto::DeliverRequest* request,
                     proto::RPCResponse* response,
                     ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_success(false);
            // DLOG(INFO) << "deliver, Node: " << request->localid() << ", sequence: " << request->sequence();
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

            std::vector<::proto::Block::SignaturePair> consensusSignatures(request->contents_size());
            // append signatures
            for (int i=0; i<request->contents_size(); i++) {
                zpp::bits::in in(request->signatures(i));
                if (failure(in(consensusSignatures[i]))) {
                    LOG(WARNING) << "Deserialize signatures failed!";
                    return;
                }
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
            if ((int)_localNodes.size() <= request->localid()) {
                LOG(WARNING) << "localId error.";
                return;
            }
            _stateMachine->OnLeaderStart(_localNodes.at(request->localid()), request->sequence());
            response->set_success(true);
        }

        void leaderChange(google::protobuf::RpcController*,
                          const proto::LeaderChangeRequest* request,
                          proto::RPCResponse* response,
                          ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            DLOG(INFO) << "leaderChanged, LocalNode: " << request->localid()
                       << ", NewLeader: " << request->newleaderid()
                       << ", sequence: " << request->sequence();
            if ((int)_localNodes.size() <= request->localid() || (int)_localNodes.size() <= request->newleaderid()) {
                LOG(WARNING) << "localId or newLeaderId error.";
                return;
            }
            _stateMachine->OnLeaderChange(_localNodes.at(request->localid()), _localNodes.at(request->newleaderid()), request->sequence());
            response->set_success(true);
        }

    private:
        // key: node id, value: node ski
        std::vector<std::shared_ptr<util::NodeConfig>> _localNodes;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<PBFTStateMachine> _stateMachine;
    };
}