//
// Created by peng on 2/15/23.
//

#pragma once

#include "brpc/server.h"
#include <memory>

namespace util {
    class DefaultRpcServer {
    public:
        // Add services into server. Notice the second parameter, because the
        // service is put on stack, we don't want server to delete it, otherwise use brpc::SERVER_OWNS_SERVICE.
        static int AddService(google::protobuf::Service* service,
                              int port,
                              const std::function<void()>& onStop=nullptr,
                              brpc::ServiceOwnership ownership=brpc::SERVER_OWNS_SERVICE) {
            std::lock_guard guard(mutex);
            auto& server = globalControlServer[port];
            if (!server) {
                server = std::make_unique<brpc::Server>();
            }
            if (server->AddService(service, ownership) != 0) {
                LOG(ERROR) << "Fail to add service!";
                return -1;
            }
            if (onStop) {
                onStopList[port].push_back(onStop);
            }
            return 0;
        }

        static int Start(int port) {
            std::lock_guard guard(mutex);
            auto& server = globalControlServer[port];
            if (!server) { return -1; }
            if (server->Start(port, nullptr) != 0) {
                LOG(ERROR) << "Fail to start HttpServer";
                return -1;
            }
            return 0;
        }

        static void Stop(int port) {
            std::lock_guard guard(mutex);
            for (auto& it: onStopList[port]) { it(); }
            onStopList[port].clear();
            globalControlServer[port].reset();
        }

        DefaultRpcServer() = delete;

    protected:
        inline static std::mutex mutex;
        inline static std::unique_ptr<brpc::Server> globalControlServer[65536];
        inline static std::vector<std::function<void()>> onStopList[65536];
    };

    class MetaRpcServer : public DefaultRpcServer {
    public:
        static inline int Start() { return DefaultRpcServer::Start(port); }

        static inline void Stop() { DefaultRpcServer::Stop(port); }

        static inline int AddService(google::protobuf::Service* service,
                                     const std::function<void()>& onStop=nullptr,
                                     brpc::ServiceOwnership ownership=brpc::SERVER_OWNS_SERVICE) {
            return DefaultRpcServer::AddService(service, port, onStop, ownership);
        }

        static int Port()  { return port; }

    private:
        static inline constexpr int port = 9500;
    };
}