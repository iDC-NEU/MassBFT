//
// Created by user on 23-3-11.
//

#pragma once

#include "peer/replicator/v2/block_sender.h"
#include "peer/replicator/v2/mr_block_receiver.h"
#include "peer/replicator/v2/zmq_port_util.h"

namespace peer {
    // Replicator has fragment sending and receiving instances,
    // and is responsible for sending and receiving fragments of nodes, all other clusters, and all local nodes.
    // It is worth noting that a Replicator is only responsible for the sending and receiving
    // of one multi-master instance, and different fragments are handled by different Replicators.
    class Replicator {
    public:
        using NodeConfigMap = std::unordered_map<int, std::vector<util::NodeConfigPtr>>;
        // ZMQConfigMap key is region id.
        using ZMQConfigMap = std::unordered_map<int, std::vector<std::shared_ptr<util::ZMQInstanceConfig>>>;

        Replicator(NodeConfigMap nodes, util::NodeConfigPtr localNode)
                :_localNodeConfig(std::move(localNode)), _nodeConfigs(std::move(nodes)) {
            for (const auto& it: _nodeConfigs) {
                _regionNodesCount[it.first] = (int)it.second.size();
            }
        }

        ~Replicator() = default;

        Replicator(const Replicator&) = delete;

        Replicator(Replicator&&) = delete;

        // optional
        void setStorage(std::shared_ptr<peer::MRBlockStorage> storage) { _localStorage = std::move(storage); }

        [[nodiscard]] auto getStorage() const { return _localStorage; }

        // need to be set
        void setBCCSP(std::shared_ptr<util::BCCSP> bccsp) { _bccsp = std::move(bccsp); }

        [[nodiscard]] auto getBCCSP() const { return _bccsp; }

        // optional
        void setBlockSenderThreadPool(std::shared_ptr<util::thread_pool_light> threadPool) {
            _blockSenderThreadPool = std::move(threadPool);
        }

        bool initialize(int portOffset=51200) {
            if (_nodeConfigs.empty() || !_localNodeConfig) {
                LOG(ERROR) << "Replicator checkAndStart failed!";
                return false;
            }
            initPortsConfig(portOffset);
            initBFG();
            initStorage();
            if (!initMRBlockReceiver()) {
                return false;
            }
            return true;
        }

        bool startReceiver(const std::unordered_map<int, proto::BlockNumber>& startAt) {
            if(!_receiver->checkAndStartService(startAt)) {
                LOG(ERROR) << "start receiver failed";
                return false;
            }
            return true;
        }

        // The receiver must be initialized first,
        // because the sender will block the connection when it initializes,
        // causing a deadlock
        bool startSender(int startAt) {
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
            if (!_bfg || !_localStorage || _nodeConfigs.empty() || _zmqPortsConfig.empty()) {
                LOG(ERROR) << "init sender failed!";
                return false;
            }
            auto& groupId = _localNodeConfig->groupId;

            ZMQConfigMap remoteReceiverConfigs;
            for (const auto& it: _regionNodesCount) {
                for (int i=0; i<it.second; i++) {
                    auto zmqInstanceConfig = std::make_unique<util::ZMQInstanceConfig>();
                    zmqInstanceConfig->nodeConfig = _nodeConfigs.at(it.first)[i];
                    zmqInstanceConfig->port = _zmqPortsConfig[it.first][i]->getRFRServerPort(groupId);
                    remoteReceiverConfigs[it.first].push_back(std::move(zmqInstanceConfig));
                }
            }

            auto sender = peer::v2::MRBlockSender::NewMRBlockSender(remoteReceiverConfigs,
                                                                    _localFragmentCfg.second,
                                                                    groupId,
                                                                    _blockSenderThreadPool);
            if (sender == nullptr) {
                LOG(ERROR) << "create sender failed";
                return false;
            }
            sender->setStorage(_localStorage);
            sender->setBFGWithConfig(_bfg, _localFragmentCfg.first);
            _sender = std::move(sender);
            return true;
        }

        bool initMRBlockReceiver() {
            if (!_bfg || !_bccsp || !_localStorage || _nodeConfigs.empty() || _zmqPortsConfig.empty()) {
                LOG(ERROR) << "init receiver failed!";
                return false;
            }
            auto& groupId = _localNodeConfig->groupId;
            auto& nodeId = _localNodeConfig->nodeId;
            auto& localZmqConfig = *_zmqPortsConfig[groupId][nodeId];
            // broadcast in the local zone, key region id, value port (as ZMQServer)
            std::unordered_map<int, int> frServerPorts = localZmqConfig.getFRServerPorts();
            // receive from crossRegionSender (as ReliableZmqServer)
            std::unordered_map<int, int> rfrServerPorts = localZmqConfig.getRFRServerPorts();

            // init _localBroadcastConfigs
            // For mr receivers, local servers broadcast ports, key is remote region id (multi-master)
            ZMQConfigMap localBroadcastConfigs;
            for (int i=0; i<_regionNodesCount[groupId]; i++) {
                for (const auto& it: _zmqPortsConfig[groupId][i]->getFRServerPorts()) {
                    auto zmqInstanceConfig = std::make_unique<util::ZMQInstanceConfig>();
                    zmqInstanceConfig->nodeConfig = _nodeConfigs.at(groupId)[i];
                    zmqInstanceConfig->port = it.second;
                    localBroadcastConfigs[it.first].push_back(std::move(zmqInstanceConfig));
                }
            }
            // create and init receiver
            auto receiver = peer::v2::MRBlockReceiver::NewMRBlockReceiver(
                    _localNodeConfig,
                    frServerPorts,   // we do not use this one yet, anything is ok
                    rfrServerPorts,   // these port are used to receive from crossRegionSender (as server)
                    localBroadcastConfigs); // the ports are used to receive from mockLocalSender (as client)
            if (receiver == nullptr) {
                LOG(ERROR) << "create receiver failed";
                return false;
            }
            receiver->setBCCSPWithThreadPool(_bccsp, _bfgAndBCCSPThreadPool);
            receiver->setStorage(_localStorage);
            receiver->setBFGWithConfig(_bfg, _localFragmentCfg.first);
            _receiver = std::move(receiver);
            return true;
        }

        void initPortsConfig(int portOffset) {
            // init frServerPorts and rfrServerPorts
            std::unordered_map<int, std::vector<std::unique_ptr<peer::v2::ZMQPortUtil>>> zmqPortsConfig;
            for (const auto& it: _regionNodesCount) {
                zmqPortsConfig[it.first].resize(it.second);
                for (int i=0; i<it.second; i++) {
                    zmqPortsConfig[it.first][i] = std::make_unique<peer::v2::ZMQPortUtil>(
                            _regionNodesCount,
                            it.first,
                            i,
                            portOffset);
                }
            }
            _zmqPortsConfig = std::move(zmqPortsConfig);
        }

        void initBFG() {
            auto& groupId = _localNodeConfig->groupId;
            auto& nodeId = _localNodeConfig->nodeId;
            _localFragmentCfg = v2::FragmentUtil::GenerateAllConfig(_regionNodesCount, groupId, nodeId);

            std::vector<peer::BlockFragmentGenerator::Config> bfgConfigList;
            for (auto& it: _localFragmentCfg.first) { // for receivers
                bfgConfigList.push_back(it.second);
            }
            bfgConfigList.push_back(_localFragmentCfg.first[nodeId]); // for senders

            _bfgAndBCCSPThreadPool = std::make_shared<util::thread_pool_light>();
            _bfg = std::make_shared<peer::BlockFragmentGenerator>(bfgConfigList, _bfgAndBCCSPThreadPool.get());
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
        // regionNodesCount is a temp value
        std::unordered_map<int, int> _regionNodesCount;
        // ports config of ALL nodes in ALL regions
        std::unordered_map<int, std::vector<std::unique_ptr<peer::v2::ZMQPortUtil>>> _zmqPortsConfig;
        // the ptr of shared storage
        std::shared_ptr<peer::MRBlockStorage> _localStorage;
        // bfg, bccsp and the corresponding thread pool.
        std::shared_ptr<BlockFragmentGenerator> _bfg;
        std::pair<v2::FragmentUtil::BFGConfigType, v2::FragmentUtil::SenderFragmentConfigType> _localFragmentCfg;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _bfgAndBCCSPThreadPool;
        // block sender and the corresponding thread pool.
        std::unique_ptr<v2::MRBlockSender> _sender;
        std::shared_ptr<util::thread_pool_light> _blockSenderThreadPool;
        // block receiver
        std::unique_ptr<v2::MRBlockReceiver> _receiver;
    };
}