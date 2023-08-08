//
// Created by user on 23-8-8.
//

#include "ca_http_server.h"
#include "nlohmann/json.hpp"
#include "httplib.h"
#include "glog/logging.h"

namespace ca {
    std::unique_ptr<ServerBackend> ServerBackend::NewServerBackend(std::unique_ptr<ServiceBackend> service) {
        auto server = std::unique_ptr<ServerBackend>(new ServerBackend);
        server->_server = std::make_unique<httplib::Server>();
        server->_service = std::move(service);
        return server;
    }

    bool ServerBackend::start(int port) {
        if (_server->listen("0.0.0.0", port)) {
            LOG(ERROR) << "Failed to start the server at port " << port;
            return false;
        }
        return true;
    }

    ServerBackend::~ServerBackend() = default;

}

