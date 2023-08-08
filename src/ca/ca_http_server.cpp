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
        server->initRoutes();
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
        });

        _server->Post("/nodes/ip", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success, json] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            try {
                auto groupId = json["group_id"];
                auto nodeId = json["node_id"];
                auto publicIp = json["pub"];
                if (json["is_client"] == true) {
                    _service->addNodeAsClient(groupId, nodeId, publicIp);
                    return;
                }
                auto privateIp = json["pri"];
                _service->setNodesIp(groupId, nodeId, publicIp, privateIp);
            } catch (...) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
        });

        _server->Post("/update/init", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<std::string>(json);
            if (!s2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            if (!_service->transmitFiles(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/update/code", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<std::string>(json);
            if (!s2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            if (!_service->updateSourcecode(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/update/pbft", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<std::string>(json);
            if (!s2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            if (!_service->updatePBFTPack(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/update/prop", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<std::string>(json);
            if (!s2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            if (!_service->updateProperties(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/compile", [&](const httplib::Request &req, httplib::Response &res) {
            auto [s1, json] = util::parseJson(req.body);
            if (!s1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [s2, nodes] = util::getListFromJson<std::string>(json);
            if (!s2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            if (!_service->compileSourcecode(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/db/generate", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success, json] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            try {
                auto name = json["name"];
                if (!_service->generateDatabase(name)) {
                    util::setErrorWithMessage(res, "execute error!");
                    return;
                }
            } catch (...) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
        });

        _server->Post("/db/backup", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->backupDatabase()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });

        _server->Post("/db/restore", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->restoreDatabase()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
        });
    }

    ServerBackend::~ServerBackend() = default;

}

