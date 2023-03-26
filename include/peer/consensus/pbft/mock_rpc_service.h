//
// Created by user on 23-3-21.
//

#pragma once

#include "common/meta_rpc_server.h"
#include "proto/pbft_connection.pb.h"
#include "common/crypto.h"
#include "proto/pbft_message.pb.h"

namespace peer::consensus {

    class MockRPCService : public proto::RPCService {
    public:
        bool checkAndStart() {
            if (util::MetaRpcServer::AddService(this, []{ }) != 0) {
                LOG(ERROR) << "Fail to add globalControlService!";
                return false;
            }
            return true;
        }

        void requestProposal(google::protobuf::RpcController*,
                             const proto::RPCRequest* request,
                             proto::RPCResponse* response,
                             ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_payload(request->payload() + " success!");
            LOG(INFO) << "receive a requestProposal, id: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }

        void verifyProposal(google::protobuf::RpcController*,
                            const proto::RPCRequest* request,
                            proto::RPCResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_payload(request->payload() + " success!");
            // LOG(INFO) << "receive a verifyProposal, id: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }

        // Not used yet
        void signProposal(google::protobuf::RpcController*,
                            const proto::RPCRequest* request,
                          proto::RPCResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            std::lock_guard guard2(mutex);
            static auto signer0 = util::OpenSSLED25519::NewFromPemString("-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIBta2stR/FYXw6HsUyiri+fMo98rn5HTmWMlYufKbBRN\n-----END PRIVATE KEY-----", "");
            if (request->localid() == 0) {
                auto ret = signer0->sign(request->payload().data(), request->payload().size());
                if (ret == std::nullopt) {
                    CHECK(false);
                }
                response->set_payload(ret->data(), ret->size());
                response->set_success(true);
            }
        }

        void deliver(google::protobuf::RpcController*,
                     const proto::DeliverRequest* request,
                     proto::RPCResponse* response,
                     ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            std::lock_guard guard2(mutex);
            response->set_payload("deliver success!");
            for (int i=0; i<request->contents_size(); i++) {
                proto::ConsensusMessage cm;
                auto& rawCm = request->contents(i);
                auto& rawSignature = request->signatures(i);
                cm.ParseFromString(rawCm);
                if (cm.sender() == 0) {
                    static auto validator = util::OpenSSLED25519::NewFromPemString("-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAGZvKfw6NtY6G4iUOV3M6xbl4uEc3ZD2HrtlTuzp04Hg=\n-----END PUBLIC KEY-----", "");
                    if(!validator) {
                        ASSERT_TRUE(false) << "validator init error";
                    }
                    util::OpenSSLED25519::digestType md;
                    CHECK(rawSignature.size() == md.size());
                    std::memcpy(md.data(), rawSignature.data(), md.size());
                    if(!validator->verify(md, rawCm.data(), rawCm.size())) {
                        CHECK(false) << "failed!" << request->sequence();
                    }
                }
            }
            // unwrap the content
            proto::Proposal proposal;
            CHECK(proposal.ParseFromString(request->proposevalue()));
            proto::TOMMessage tomMessage;
            CHECK(tomMessage.ParseFromString(proposal.message()));
            // LOG(INFO) << tomMessage.content();

            response->set_success(true);
        }
        std::mutex mutex;

        void leaderStart(google::protobuf::RpcController*,
                         const proto::RPCRequest* request,
                         proto::RPCResponse* response,
                         ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_payload(request->payload() + " success!");
            LOG(INFO) << "receive a leaderStart, id: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }

        void leaderStop(google::protobuf::RpcController*,
                        const proto::RPCRequest* request,
                        proto::RPCResponse* response,
                        ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_payload(request->payload() + " success!");
            LOG(INFO) << "receive a leaderStop, id: " << request->localid() << ", sequence: " << request->sequence();
            response->set_success(true);
        }
    };
}