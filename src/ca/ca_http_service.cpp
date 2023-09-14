//
// Created by user on 23-8-8.
//

#include "ca/ca_http_service.h"

namespace ca {
    std::unique_ptr<ServiceBackend> ServiceBackend::NewServiceBackend(std::unique_ptr<ca::Dispatcher> dispatcher) {
        auto service = std::unique_ptr<ServiceBackend>(new ServiceBackend);
        service->_dispatcher = std::move(dispatcher);
        return service;
    }

    void ServiceBackend::initNodes(const std::vector<int> &nodes) {
        _initializer = std::make_unique<ca::Initializer>(nodes);
        _initializer->initDefaultConfig();
        _dispatcher->overrideProperties();
    }

    void ServiceBackend::setNodesIp(int groupId, int nodeId, const std::string &pub, const std::string &pri) {
        ca::Initializer::SetNodeIp(groupId, nodeId, pub, pri);
        NodeConfig cfg{};
        cfg.groupId = groupId;
        cfg.nodeId = nodeId;
        cfg.isClient = false;
        _nodesList[pub] = cfg;
    }

    void ServiceBackend::addNodeAsClient(int groupId, int nodeId, const std::string &pub) {
        NodeConfig cfg{};
        cfg.groupId = groupId;
        cfg.nodeId = nodeId;
        cfg.isClient = true;
        _nodesList[pub] = cfg;
    }

    void ServiceBackend::updateProperties(bool clientOnly) {
        for (const auto& it: _nodesList) {
            const auto& cfg = it.second;
            if (clientOnly && !cfg.isClient) {
                continue;
            }
            ca::Initializer::SetLocalId(cfg.groupId, cfg.nodeId);
            // transmit config to remote
            bool success = false;
            for (int i=0; i<3; i++) {
                if (_dispatcher->transmitPropertiesToRemote(it.first)) {
                    success = true;
                    break;
                }
            }
            if (!success) {
                LOG(ERROR) << "Failed to connect to specific ip: " << it.first;
            }
        }
    }

    bool ServiceBackend::hello() {
        for (int i=0; i<3; i++) {
            bool success = true;
            for (const auto &it: _nodesList) {
                if (!_dispatcher->hello(it.first)) {
                    success = false;
                    break;
                }
            }
            if (success) {
                return true;
            }
        }
        return false;
    }

    ServiceBackend::~ServiceBackend() = default;
}

