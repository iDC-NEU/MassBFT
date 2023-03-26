//
// Created by peng on 2/15/23.
//

#pragma once

#include "brpc/server.h"
#include <memory>

namespace util {
    template<int port=9500>
    class DefaultRpcServer {
    public:
        // Add services into server. Notice the second parameter, because the
        // service is put on stack, we don't want server to delete it, otherwise use brpc::SERVER_OWNS_SERVICE.
        static int AddService(google::protobuf::Service* service, const std::function<void()>& onStop=nullptr, brpc::ServiceOwnership ownership=brpc::SERVER_OWNS_SERVICE) {
            std::lock_guard guard(mutex);
            if (!globalControlServer) {
                globalControlServer = std::make_unique<brpc::Server>();
            }
            if (globalControlServer->AddService(service, ownership) != 0) {
                LOG(ERROR) << "Fail to add service!";
                return -1;
            }
            if (onStop) {
                onStopList.push_back(onStop);
            }
            return 0;
        }

        static int Start() {
            std::lock_guard guard(mutex);
            if (!globalControlServer) {
                return -1;
            }
            if (globalControlServer->Start(port, nullptr) != 0) {
                LOG(ERROR) << "Fail to start HttpServer";
                return -1;
            }
            return 0;
        }

        static void Stop() {
            std::lock_guard guard(mutex);
            for (auto& it: onStopList) {
                it();
            }
            globalControlServer->Stop(0);
            globalControlServer.reset();
        }

    private:
        inline static std::mutex mutex;
        inline static std::unique_ptr<brpc::Server> globalControlServer = nullptr;
        inline static std::vector<std::function<void()>> onStopList;
    };

    using MetaRpcServer = DefaultRpcServer<>;
}