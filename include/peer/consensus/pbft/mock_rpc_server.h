//
// Created by user on 23-3-21.
//

#pragma once

#include "common/meta_rpc_server.h"
#include "proto/pbft_connection.pb.h"

namespace peer::consensus {

    class RPCServiceImpl : public util::RPCService {
    public:
        void verifyProposal(google::protobuf::RpcController*,
                            const ::util::RPCRequest* request,
                            ::util::RPCResponse* response,
                            ::google::protobuf::Closure* done) override {
            brpc::ClosureGuard guard(done);
            response->set_payload(request->payload() + " success!");
            LOG(INFO) << "receive a verifyProposal, id: " << request->localid()
                      << ", sequence: " << request->sequence();
            response->set_success(true);
        }

    };

    class MockRPCServer {
    public:
        static int AddRPCService() {
            auto* service = new RPCServiceImpl();
            // Add services into server. Notice the second parameter, because the
            // service is put on stack, we don't want server to delete it, otherwise use brpc::SERVER_OWNS_SERVICE.
            if (util::MetaRpcServer::AddService(service, []{ globalControlService = nullptr; }) != 0) {
                LOG(ERROR) << "Fail to add globalControlService!";
                return -1;
            }
            globalControlService = service;
            return 0;
        }

        // globalControlServer own globalControlService
        inline static RPCServiceImpl* globalControlService = nullptr;
    };
}