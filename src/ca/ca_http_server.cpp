//
// Created by user on 23-8-8.
//

#include "ca/ca_http_server.h"
#include "nlohmann/json.hpp"
#include "httplib.h"
#include "glog/logging.h"

namespace ca {
    std::unique_ptr<ServerBackend> ServerBackend::NewServerBackend(std::unique_ptr<ServiceBackend> service,
                                                                   std::shared_ptr<httplib::Server> httpServer) {
        auto server = std::unique_ptr<ServerBackend>(new ServerBackend);
        server->_server = std::move(httpServer);
        server->_service = std::move(service);
        return server;
    }

    ServerBackend::~ServerBackend() = default;

}

