//
// Created by user on 23-6-29.
//

#include "peer/core/user_rpc_controller.h"
#include "peer/storage/mr_block_storage.h"
#include "common/meta_rpc_server.h"

namespace peer::core {
    bool peer::core::UserRPCController::NewRPCController(std::shared_ptr<peer::MRBlockStorage> storage, int rpcPort) {
        auto service = new UserRPCController();
        service->_storage = std::move(storage);
        service->_rpcServerPort = rpcPort;
        if (util::DefaultRpcServer::AddService(service, rpcPort) != 0) {
            LOG(ERROR) << "Fail to add globalControlService!";
            return false;
        }
        return true;
    }

    UserRPCController::~UserRPCController() = default;

    bool UserRPCController::StartRPCService(int rpcPort) {
        if (util::DefaultRpcServer::Start(rpcPort) != 0) {
            LOG(ERROR) << "Fail to start DefaultRpcServer at port: " << rpcPort;
            return false;
        }
        return true;
    }

    void UserRPCController::StopRPCService(int rpcPort) {
        util::DefaultRpcServer::Stop(rpcPort);
    }

    void UserRPCController::hello(::google::protobuf::RpcController *,
                                  const ::ycsb::client::proto::HelloRequest *request,
                                  ::ycsb::client::proto::HelloResponse *response, ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(true);
        LOG(INFO) << "Receive hello from " << request->ski();
    }

    void UserRPCController::pullBlock(::google::protobuf::RpcController *,
                                      const ::ycsb::client::proto::PullBlockRequest *request,
                                      ::ycsb::client::proto::PullBlockResponse *response,
                                      ::google::protobuf::Closure *done) {
        brpc::ClosureGuard guard(done);
        response->set_success(false);
        DLOG(INFO) << "pullBlock, chainId: " << request->chainid()  << ", blockId: " << request->blockid();
        auto timeout = -1;
        if (request->has_timeoutms()) {
            timeout = request->timeoutms();
        }
        auto block = _storage->waitForBlock(request->chainid(), request->blockid(), timeout);
        if (block == nullptr) {
            response->set_payload("Failed to get block within timeout.");
            return;
        }
        if (block->haveSerializedMessage()) {
            response->set_payload(*block->getSerializedMessage());
        } else {
            auto* payload = response->mutable_payload();
            auto ret = block->serializeToString(payload);
            if (!ret.valid) {
                response->set_payload("Serialize message failed.");
                return;
            }
        }
        response->set_success(true);
    }
}
