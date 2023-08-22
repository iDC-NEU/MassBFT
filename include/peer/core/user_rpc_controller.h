//
// Created by user on 23-6-29.
//

#pragma once

#include "proto/user_connection.pb.h"
#include <memory>

namespace peer {
    class BlockLRUCache;
}

namespace peer::core {
    namespace inner {
        struct ControllerImpl;
    }

    class UserRPCController  : public ::client::proto::UserService {
    public:
        static bool NewRPCController(std::shared_ptr<peer::BlockLRUCache> storage, int rpcPort);

        ~UserRPCController() override;

        [[nodiscard]] static bool StartRPCService(int rpcPort);

        static void StopRPCService(int rpcPort);

    protected:
        UserRPCController() = default;

        void hello(::google::protobuf::RpcController* controller,
                   const ::client::proto::HelloRequest* request,
                   ::client::proto::HelloResponse* response,
                   ::google::protobuf::Closure* done) override;

        void getBlock(::google::protobuf::RpcController* controller,
                      const ::client::proto::GetBlockRequest* request,
                      ::client::proto::GetBlockResponse* response,
                      ::google::protobuf::Closure* done) override;

        void getLightBlock(::google::protobuf::RpcController* controller,
                      const ::client::proto::GetBlockRequest* request,
                      ::client::proto::GetBlockResponse* response,
                      ::google::protobuf::Closure* done) override;

        void getTop(::google::protobuf::RpcController* controller,
                    const ::client::proto::GetTopRequest* request,
                    ::client::proto::GetTopResponse* response,
                    ::google::protobuf::Closure* done) override;

        void getTxWithProof(::google::protobuf::RpcController* controller,
                            const ::client::proto::GetTxRequest* request,
                            ::client::proto::GetTxResponse* response,
                            ::google::protobuf::Closure* done) override;

        void getBlockHeader(::google::protobuf::RpcController* controller,
                            const ::client::proto::GetBlockHeaderRequest* request,
                            ::client::proto::GetBlockHeaderResponse* response,
                            ::google::protobuf::Closure* done) override;

    private:
        std::unique_ptr<inner::ControllerImpl> _impl;
    };
}