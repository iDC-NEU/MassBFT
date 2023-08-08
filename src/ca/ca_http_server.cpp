//
// Created by user on 23-8-8.
//

#include "ca/ca_http_server.h"
#include "ca/ca_http_service.h"
#include "common/http_util.h"
#include "glog/logging.h"

namespace ca {
    std::unique_ptr<ServerBackend> ServerBackend::NewServerBackend(std::unique_ptr<ServiceBackend> service,
                                                                   std::shared_ptr<httplib::Server> httpServer) {
        auto server = std::unique_ptr<ServerBackend>(new ServerBackend);
        server->_server = std::move(httpServer);
        server->_service = std::move(service);
        return server;
    }

    void ServerBackend::initRoutes() {
        _server->Post("/nodes/init", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<int>(json);
            if (!s2 || nodes.empty()) {
                util::setErrorWithMessage(res, "initNodes param contains error!");
                return;
            }
            _service->initNodes(nodes);
            util::setSuccessWithMessage(res, "Operation complete.");
        });

    }

    ServerBackend::~ServerBackend() = default;

}

