//
// Created by user on 23-8-8.
//

#include "ca/ca_http_server.h"
#include "ca/ca_http_service.h"
#include "common/http_util.h"
#include "common/property.h"
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
        _server->Post("/config/init", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success1, json] = util::parseJson(req.body);
            if (!success1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [success2, nodes] = util::getListFromJson<int>(json);
            if (!success2 || nodes.empty()) {
                util::setErrorWithMessage(res, "initNodes param contains error!");
                return;
            }
            std::stringstream ss;
            std::for_each(nodes.begin(), nodes.end(), [&](const auto& t) { ss << t << ", "; });
            LOG(INFO) << "Reset all config: " << "{" << ss.str() << "}";
            _service->initNodes(nodes);
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/config/node", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success, json] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            if (!addNode(json)) {
                util::setErrorWithMessage(res, "Add the node failed!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/config/nodes", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success1, json] = util::parseJson(req.body);
            if (!success1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [success2, nodes] = util::getListFromJson<nlohmann::json>(json);
            if (!success2 || nodes.empty()) {
                util::setErrorWithMessage(res, "Param contains error!");
                return;
            }
            for (const auto& it: nodes) {
                if (!addNode(it)) {
                    util::setErrorWithMessage(res, "Add the node failed!");
                    return;
                }
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/upload/all", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success1, json] = util::parseJson(req.body);
            if (!success1) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            auto [success2, nodes] = util::getListFromJson<std::string>(json);
            if (!success2) {
                util::setErrorWithMessage(res, "param contains error!");
                return;
            }
            LOG(INFO) << "Resetting all files on remote nodes.";
            if (!_service->transmitFiles(nodes)) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
            LOG(INFO) << "Operation complete successfully.";
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/upload/code", [&](const httplib::Request &req, httplib::Response &res) {
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
            LOG(INFO) << "Operation complete successfully.";
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/upload/pbft", [&](const httplib::Request &req, httplib::Response &res) {
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
            LOG(INFO) << "Operation complete successfully.";
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/upload/prop", [&](const httplib::Request &req, httplib::Response &res) {
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
            _service->updateProperties(nodes);
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/upload/compile", [&](const httplib::Request &req, httplib::Response &res) {
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
            LOG(INFO) << "Operation complete successfully.";
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/generate", [&](const httplib::Request &req, httplib::Response &res) {
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
            LOG(INFO) << "Operation complete successfully.";
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/backup", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->backupDatabase()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/restore", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->restoreDatabase()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/start", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->startPeer()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/stop", [&](const httplib::Request &, httplib::Response &res) {
            if (!_service->stopPeer()) {
                util::setErrorWithMessage(res, "execute error!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/user/start", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success, json] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            try {
                auto name = json["name"];
                if (!_service->startUser(name)) {
                    util::setErrorWithMessage(res, "execute error!");
                    return;
                }
            } catch (...) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            util::setSuccessWithMessage(res, "Operation complete successfully.");
        });

        _server->Post("/peer/config", [&](const httplib::Request &req, httplib::Response &res) {
            auto [success, json] = util::parseJson(req.body);
            if (!success) {
                util::setErrorWithMessage(res, "Body deserialize failed!");
                return;
            }
            try {
                std::string parent = json["parent"];
                std::string key = json["key"];
                auto value = json["value"];
                util::setSuccessWithMessage(res, "Operation complete successfully.");
                if (parent.empty()) {
                    if (value.is_number_integer()) {
                        util::Properties::SetProperties(key, int(value));
                        return;
                    }
                    if (value.is_number_float()) {
                        util::Properties::SetProperties(key, double(value));
                        return;
                    }
                    if (value.is_string()) {
                        util::Properties::SetProperties(key, std::string(value));
                        return;
                    }
                    if (value.is_boolean()) {
                        util::Properties::SetProperties(key, bool(value));
                        return;
                    }
                } else {
                    auto* properties = util::Properties::GetProperties();
                    auto node = properties->getCustomProperties(parent);
                    if (value.is_number_integer()) {
                        node[key] = int(value);
                        return;
                    }
                    if (value.is_number_float()) {
                        node[key] = double(value);
                        return;
                    }
                    if (value.is_string()) {
                        node[key] = std::string(value);
                        return;
                    }
                    if (value.is_boolean()) {
                        node[key] = bool(value);
                        return;
                    }
                }
            } catch (...) { }
            util::setErrorWithMessage(res, "Body deserialize failed!");
        });
    }

    bool ServerBackend::addNode(const nlohmann::json& json) {
        try {
            const auto& groupId = json["group_id"];
            const auto& nodeId = json["node_id"];
            const auto& publicIp = json["pub"];
            if (json["is_client"] == true) {
                LOG(INFO) << "Add node as client: "  << groupId << ", " << nodeId << ", " << publicIp;
                _service->addNodeAsClient(groupId, nodeId, publicIp);
            } else {
                const auto& privateIp = json["pri"];
                LOG(INFO) << "Add node as server: "  << groupId << ", " << nodeId << ", " << publicIp << ", " << privateIp;
                _service->setNodesIp(groupId, nodeId, publicIp, privateIp);
            }
        } catch (...) {
            return false;
        }
        return true;
    }

    ServerBackend::~ServerBackend() = default;

}

