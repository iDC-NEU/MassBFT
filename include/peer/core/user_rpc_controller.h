//
// Created by user on 23-6-29.
//

#pragma once

#include "proto/user_connection.pb.h"
#include <memory>

namespace peer {
    class MRBlockStorage;
}

namespace peer::core {
    class UserRPCController  : public ::ycsb::client::proto::UserService {
    public:
        static bool NewRPCController(std::shared_ptr<peer::MRBlockStorage> storage, int rpcPort);

        ~UserRPCController() override;

        [[nodiscard]] static bool StartRPCService(int rpcPort);

        static void StopRPCService(int rpcPort);

    protected:
        UserRPCController() = default;

        void hello(::google::protobuf::RpcController* controller,
                   const ::ycsb::client::proto::HelloRequest* request,
                   ::ycsb::client::proto::HelloResponse* response,
                   ::google::protobuf::Closure* done) override;

        void pullBlock(::google::protobuf::RpcController* controller,
                       const ::ycsb::client::proto::PullBlockRequest* request,
                       ::ycsb::client::proto::PullBlockResponse* response,
                       ::google::protobuf::Closure* done) override;

    private:
        int _rpcServerPort = -1;
        std::shared_ptr<peer::MRBlockStorage> _storage;
    };
}