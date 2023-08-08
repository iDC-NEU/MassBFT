//
// Created by user on 23-8-8.
//

#include "ca/ca_http_service.h"

namespace ca {
    std::unique_ptr<ServiceBackend> ServiceBackend::NewServiceBackend(std::unique_ptr<ca::Dispatcher> dispatcher) {
        auto service = std::unique_ptr<ServiceBackend>(new ServiceBackend);
        service->_dispatcher = std::move(dispatcher);
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

    bool ServiceBackend::updateProperties() {
        for (const auto& it: _nodesList) {
            const auto& cfg = it.second;
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

    bool ServiceBackend::updateProperties(const std::vector<std::string> &ipList) {
        for (const auto& it: ipList) {
            if (!_nodesList.contains(it)) {
                LOG(ERROR) << "Does not exist specific ip: " << it;
                continue;
            }
            const auto& cfg = _nodesList[it];
            ca::Initializer::SetLocalId(cfg.groupId, cfg.nodeId);
            // transmit config to remote
            bool success = false;
            for (int i=0; i<3; i++) {
                if (_dispatcher->transmitPropertiesToRemote(it)) {
                    success = true;
                    break;
                }
            }
            if (!success) {
                LOG(ERROR) << "Failed to connect to specific ip: " << it;
            }

        }
    }
    ServiceBackend::~ServiceBackend() = default;
}

