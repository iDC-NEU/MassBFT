//
// Created by user on 23-3-11.
//

#pragma once

#include "peer/replicator/direct/direct_block_sender.h"
#include "peer/replicator/direct/direct_block_receiver.h"
#include "common/zmq_port_util.h"

namespace peer::direct {
    class Replicator {
    public:
        using NodeConfigMap = std::unordered_map<int, std::vector<util::NodeConfigPtr>>;
        // ZMQConfigMap key is region id.
        using ZMQConfigMap = std::unordered_map<int, std::vector<std::shared_ptr<util::ZMQInstanceConfig>>>;

        Replicator(NodeConfigMap nodes, util::NodeConfigPtr localNode)
                :_localNodeConfig(std::move(localNode)), _nodeConfigs(std::move(nodes)) { }

        ~Replicator() = default;

        Replicator(const Replicator&) = delete;

        Replicator(Replicator&&) = delete;

        // optional
        void setStorage(std::shared_ptr<peer::MRBlockStorage> storage) { _localStorage = std::move(storage); }

        [[nodiscard]] auto getStorage() const { return _localStorage; }

        // need to be set
        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _bccspThreadPool = std::move(threadPool);
        }

        [[nodiscard]] auto getBCCSP() const { return _bccsp; }

        // _zmqPortsConfig = util::ZMQPortUtil::InitPortsConfig(portOffset, regionNodesCount, samePort);
        void setPortUtilMap(std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> zmqPortsConfig) {
            _zmqPortsConfig = std::move(zmqPortsConfig);
        }

        bool initialize() {
            if (_nodeConfigs.empty() || !_localNodeConfig) {
                LOG(ERROR) << "Replicator checkAndStart failed!";
                return false;
            }
            initStorage();
            if (!initMRBlockReceiver()) {
                return false;
            }
            return true;
        }

        bool startReceiver(const std::unordered_map<int, proto::BlockNumber>&) {
            if(!_receiver->checkAndStartService()) {
                LOG(ERROR) << "start receiver failed";
                return false;
            }
            return true;
        }

        // The receiver must be initialized first,
        // because the sender will block the connection when it initializes,
        // causing a deadlock
        bool startSender(int startAt) {
            if (_localNodeConfig->nodeId != 0) {
                LOG(INFO) << "Not group leader, skip start sender!";
                return true;
            }
            if (!initMRBlockSender()) {
                return false;
            }
            if(!_sender->checkAndStart(startAt)) {
                LOG(ERROR) << "start sender failed";
                return false;
            }
            return true;
        }

    protected:
        bool initMRBlockSender() {
            if (!_localStorage || _nodeConfigs.empty() || !_zmqPortsConfig) {
                LOG(ERROR) << "init sender failed!";
                return false;
            }
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

            auto sender = peer::direct::MRBlockSender::NewMRBlockSender(remoteReceiverConfigs, groupId);
            if (sender == nullptr) {
                LOG(ERROR) << "create sender failed";
                return false;
            }
            sender->setStorage(_localStorage);
            _sender = std::move(sender);
            return true;
        }

        bool initMRBlockReceiver() {
            if (!_bccsp || !_localStorage || _nodeConfigs.empty() || !_zmqPortsConfig) {
                LOG(ERROR) << "init receiver failed!";
                return false;
            }
            auto& groupId = _localNodeConfig->groupId;
            auto& nodeId = _localNodeConfig->nodeId;
            auto& localZmqConfig = *_zmqPortsConfig->at(groupId)[nodeId];
            // broadcast in the local zone, key region id, value port (as ZMQServer)
            auto frServerPorts = localZmqConfig.getRemoteServicePorts(util::PortType::LOCAL_FRAGMENT_BROADCAST);
            // receive from crossRegionSender (as ReliableZmqServer)
            auto rfrServerPorts = localZmqConfig.getRemoteServicePorts(util::PortType::REMOTE_FRAGMENT_RECEIVE);

            // init _localBroadcastConfigs
            // For mr receivers, local servers broadcast ports, key is remote region id (multi-master)
            ZMQConfigMap localBroadcastConfigs;
            for (int i=0; i<(int)_nodeConfigs.at(groupId).size(); i++) {
                for (const auto& it: _zmqPortsConfig->at(groupId)[i]->getRemoteServicePorts(util::PortType::LOCAL_FRAGMENT_BROADCAST)) {
                    auto zmqInstanceConfig = std::make_unique<util::ZMQInstanceConfig>();
                    zmqInstanceConfig->nodeConfig = _nodeConfigs.at(groupId)[i];
                    zmqInstanceConfig->port = it.second;
                    localBroadcastConfigs[it.first].push_back(std::move(zmqInstanceConfig));
                }
            }
            // create and init receiver
            auto receiver = peer::direct::MRBlockReceiver::NewMRBlockReceiver(
                    _localNodeConfig,
                    frServerPorts,   // we do not use this one yet, anything is ok
                    rfrServerPorts,   // these port are used to receive from crossRegionSender (as server)
                    localBroadcastConfigs); // the ports are used to receive from mockLocalSender (as client)
            if (receiver == nullptr) {
                LOG(ERROR) << "create receiver failed";
                return false;
            }
            receiver->setBCCSPWithThreadPool(_bccsp, _bccspThreadPool);
            receiver->setStorage(_localStorage);
            _receiver = std::move(receiver);
            return true;
        }

        void initStorage() {
            if (_localStorage != nullptr) {
                LOG(INFO) << "storage has already inited!";
                return;
            }
            int maxRegionId = 0;
            for(const auto& it: _nodeConfigs) {
                maxRegionId = std::max(it.first, maxRegionId);
            }
            _localStorage = std::make_shared<peer::MRBlockStorage>(maxRegionId+1);
        }

    private:
        // the config of local node
        const util::NodeConfigPtr _localNodeConfig;
        const NodeConfigMap _nodeConfigs;
        // ports config of ALL nodes in ALL regions
        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> _zmqPortsConfig;
        // the ptr of shared storage
        std::shared_ptr<peer::MRBlockStorage> _localStorage;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _bccspThreadPool;
        std::unique_ptr<MRBlockSender> _sender;
        std::unique_ptr<MRBlockReceiver> _receiver;
    };
}