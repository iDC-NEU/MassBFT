//
// Created by user on 23-3-11.
//

#pragma once

#include "peer/replicator/multyway_only/multiway_block_sender.h"
#include "peer/replicator/direct/direct_block_receiver.h"
#include "common/zmq_port_util.h"

namespace peer::multiway {
    class Replicator {
    public:
        using NodeConfigMap = std::unordered_map<int, std::vector<util::NodeConfigPtr>>;
        using ZMQConfigMap = std::unordered_map<int, std::vector<std::shared_ptr<util::ZMQInstanceConfig>>>;

        Replicator(NodeConfigMap nodes, util::NodeConfigPtr localNode)
                :_localNodeConfig(std::move(localNode)), _nodeConfigs(std::move(nodes)) { }

        ~Replicator() = default;

        Replicator(const Replicator&) = delete;

        Replicator(Replicator&&) = delete;

        void setStorage(std::shared_ptr<peer::MRBlockStorage> storage) { _localStorage = std::move(storage); }

        [[nodiscard]] auto getStorage() const { return _localStorage; }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _bccspThreadPool = std::move(threadPool);
        }

        [[nodiscard]] auto getBCCSP() const { return _bccsp; }

        void setPortUtilMap(std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> zmqPortsConfig) {
            _zmqPortsConfig = std::move(zmqPortsConfig);
        }

        bool initialize() {
            initStorage();
            return initMRBlockReceiver();
        }

        bool startReceiver(const std::unordered_map<int, proto::BlockNumber>&) {
            return _receiver->checkAndStartService();
        }

        bool startSender(int startAt) {
            if (!initMRBlockSender()) {
                return false;
            }
            return _sender->checkAndStart(startAt);
        }

    protected:
        bool initMRBlockSender() {
            auto& groupId = _localNodeConfig->groupId;
            ZMQConfigMap remoteReceiverConfigs;
            for (const auto& it: _nodeConfigs) {
                for (int i=0; i<(int)it.second.size(); i++) {
                    auto zmqInstanceConfig = std::make_unique<util::ZMQInstanceConfig>();
                    zmqInstanceConfig->nodeConfig = _nodeConfigs.at(it.first)[i];
                    zmqInstanceConfig->port = _zmqPortsConfig->at(it.first)[i]->getRemoteServicePort(util::PortType::REMOTE_FRAGMENT_RECEIVE, groupId);
                    remoteReceiverConfigs[it.first].push_back(std::move(zmqInstanceConfig));
                }
            }

            auto sender = peer::multiway::MRBlockSender::NewMRBlockSender(remoteReceiverConfigs, groupId, _localNodeConfig->nodeId);
            if (sender == nullptr) {
                return false;
            }
            sender->setStorage(_localStorage);
            _sender = std::move(sender);
            return true;
        }

        bool initMRBlockReceiver() {
            auto& groupId = _localNodeConfig->groupId;
            auto& nodeId = _localNodeConfig->nodeId;
            auto& localZmqConfig = *_zmqPortsConfig->at(groupId)[nodeId];
            auto frServerPorts = localZmqConfig.getRemoteServicePorts(util::PortType::LOCAL_FRAGMENT_BROADCAST);
            auto rfrServerPorts = localZmqConfig.getRemoteServicePorts(util::PortType::REMOTE_FRAGMENT_RECEIVE);

            ZMQConfigMap localBroadcastConfigs;
            for (int i=0; i<(int)_nodeConfigs.at(groupId).size(); i++) {
                for (const auto& it: _zmqPortsConfig->at(groupId)[i]->getRemoteServicePorts(util::PortType::LOCAL_FRAGMENT_BROADCAST)) {
                    auto zmqInstanceConfig = std::make_unique<util::ZMQInstanceConfig>();
                    zmqInstanceConfig->nodeConfig = _nodeConfigs.at(groupId)[i];
                    zmqInstanceConfig->port = it.second;
                    localBroadcastConfigs[it.first].push_back(std::move(zmqInstanceConfig));
                }
            }
            auto receiver = peer::direct::MRBlockReceiver::NewMRBlockReceiver(
                    _localNodeConfig,
                    frServerPorts,
                    rfrServerPorts,
                    localBroadcastConfigs);
            if (receiver == nullptr) {
                return false;
            }
            receiver->setBCCSPWithThreadPool(_bccsp, _bccspThreadPool);
            receiver->setStorage(_localStorage);
            _receiver = std::move(receiver);
            return true;
        }

        void initStorage() {
            if (_localStorage != nullptr) {
                return;
            }
            int maxRegionId = 0;
            for(const auto& it: _nodeConfigs) {
                maxRegionId = std::max(it.first, maxRegionId);
            }
            _localStorage = std::make_shared<peer::MRBlockStorage>(maxRegionId+1);
        }

    private:
        const util::NodeConfigPtr _localNodeConfig;
        const NodeConfigMap _nodeConfigs;
        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> _zmqPortsConfig;
        std::shared_ptr<peer::MRBlockStorage> _localStorage;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _bccspThreadPool;
        std::unique_ptr<MRBlockSender> _sender;
        std::unique_ptr<peer::direct::MRBlockReceiver> _receiver;
    };
}