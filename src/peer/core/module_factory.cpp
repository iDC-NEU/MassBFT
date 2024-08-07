//
// Created by user on 23-5-8.
//

#include "peer/core/module_factory.h"
#include "peer/core/user_rpc_controller.h"
#include "peer/consensus/block_order/global_ordering.h"
#include "peer/consensus/block_order/round_based/round_based_block_order.h"
#include "peer/consensus/block_order/geobft/geobft_block_order.h"
#include "peer/consensus/block_order/steward/steward_block_order.h"
#include "peer/consensus/block_order/iss/iss_block_order.h"
#include "peer/consensus/pbft/local_consensus_controller.h"
#include "peer/consensus/pbft/single_pbft_controller.h"
#include "peer/replicator/replicator.h"
#include "peer/replicator/direct/direct_replicator.h"
#include "peer/replicator/multyway_only/multiway_replicator.h"
#include "common/yaml_key_storage.h"
#include "common/property.h"

namespace peer::core {
    ModuleFactory::~ModuleFactory() = default;

    std::unique_ptr<ModuleFactory> ModuleFactory::NewModuleFactory(const std::shared_ptr<util::Properties>& properties) {
        std::unique_ptr<ModuleFactory> mf(new ModuleFactory);
        mf->_properties = properties;
        return mf;
    }

    std::pair<std::shared_ptr<::util::BCCSP>, std::shared_ptr<::util::thread_pool_light>> ModuleFactory::getOrInitBCCSPAndThreadPool() {
        if (_bccsp && _threadPoolForBCCSP) {
            return { _bccsp, _threadPoolForBCCSP };
        }
        auto node = _properties->getCustomProperties("bccsp");
        _bccsp = std::make_shared<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
        _threadPoolForBCCSP = std::make_shared<util::thread_pool_light>(_properties->getBCCSPWorkerCount(), "bccsp_tp");
        return { _bccsp, _threadPoolForBCCSP };
    }

    std::shared_ptr<::peer::MRBlockStorage> ModuleFactory::getOrInitContentStorage() {
        if (_contentStorage) {
            return _contentStorage;
        }
        auto gc = _properties->getNodeProperties().getGroupCount();
        if (gc <= 0) {
            return nullptr;
        }
        _contentStorage = std::make_shared<peer::MRBlockStorage>(gc);
        return _contentStorage;
    }

    std::shared_ptr<ModuleFactory::ReplicatorType> ModuleFactory::getOrInitReplicator() {
        if (_replicator) {
            return _replicator;
        }
        auto np = _properties->getNodeProperties();
        std::unordered_map<int, std::vector<util::NodeConfigPtr>> nodes;
        for (int i=0; i<np.getGroupCount(); i++) {
            // Cross-domain block transmission needs to transmit the erasure code
            // segment to the corresponding remote node, so a public network address is required
            nodes[i] = np.getGroupNodesInfo(i);
        }
        auto localNode = np.getLocalNodeInfo();
        if (localNode == nullptr) {
            return nullptr;
        }
        auto replicator = std::make_shared<ReplicatorType>(nodes, localNode);
        auto [bccsp, tp] = getOrInitBCCSPAndThreadPool();
        replicator->setBCCSPWithThreadPool(std::move(bccsp), std::move(tp));
        auto cs = getOrInitContentStorage();
        if (!cs) {
            return nullptr;
        }
        replicator->setStorage(std::move(cs));
        auto pum = getOrInitZMQPortUtilMap();
        if (!pum) {
            return nullptr;
        }
        replicator->setPortUtilMap(std::move(pum));
        if (!replicator->initialize()) {
            LOG(WARNING) << "replicator initialize error!";
            return nullptr;
        }
        std::unordered_map<int, proto::BlockNumber> startAt;
        for (const auto& it: nodes) {
            startAt[it.first] = _properties->getStartBlockNumber(it.first);
        }
        if (!replicator->startReceiver(startAt)) {
            return nullptr;
        }
        _replicator = replicator;
        return replicator;
    }

    std::unique_ptr<BFTController> ModuleFactory::newReplicatorBFTController(int groupId) {
        auto np = _properties->getNodeProperties();
        auto localNode = np.getLocalNodeInfo();
        auto [user, pass, success] = _properties->getSSHInfo();
        if (!success) {
            LOG(WARNING) << "please check your ssh setting in config file.";
        }
        auto runningPath = _properties->getRunningPath();
        peer::consensus::SSHConfig sshConfig {
                .ip = localNode->priIp,
                .port = -1,
                .userName = user,
                .password = pass,
        };
        auto ic = peer::consensus::BFTInstanceController::NewBFTInstanceController(
                sshConfig,
                groupId,
                localNode->nodeId,
                runningPath,
                _properties->getJVMPath());
        if (!ic) {
            return nullptr;
        }
        // generate host file
        auto portMap = getOrInitZMQPortUtilMap();
        if (!portMap) {
            return nullptr;
        }
        std::vector<peer::consensus::NodeHostConfig> hostList;
        auto& groupPortMap = portMap->at(localNode->groupId);
        auto localRegionNodes = np.getGroupNodesInfo(localNode->groupId);
        CHECK(localRegionNodes.size() == portMap->at(localNode->groupId).size());
        for (int i=0; i<(int)localRegionNodes.size(); i++) {
            auto& node = localRegionNodes[i];
            hostList.push_back({
                                       .processId = node->nodeId,
                                       .ip = node->priIp,
                                       .serverToServerPort = groupPortMap[i]->getLocalServicePorts(util::PortType::SERVER_TO_SERVER)[i],
                                       .serverToClientPort = groupPortMap[i]->getLocalServicePorts(util::PortType::CLIENT_TO_SERVER)[i],
                                       .rpcPort =  groupPortMap[i]->getLocalServicePorts(util::PortType::BFT_RPC)[i],
                               });
        }
        ic->prepareConfigurationFile(hostList);
        // ----- init LocalPBFTController ----
        auto [bccsp, tp] = this->getOrInitBCCSPAndThreadPool();
        auto cs = getOrInitContentStorage();
        if (!cs) {
            return nullptr;
        }
        // local region nodes
        auto pc = consensus::v2::LocalConsensusController::NewLocalConsensusController(
                localRegionNodes,
                localNode->nodeId,
                groupPortMap.at(localNode->nodeId),
                bccsp,
                std::move(tp),
                std::move(cs),
                _properties->getBlockBatchTimeoutMs(),
                _properties->getBlockMaxBatchSize());
        if (!pc || !pc->startRPCService()) {
            return nullptr;
        }
        auto rc = std::make_unique<consensus::v2::SinglePBFTController>(std::move(ic), localNode->groupId, localNode->nodeId, groupId);

        return std::make_unique<BFTController>(std::move(pc), std::move(rc));
    }

    std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> ModuleFactory::getOrInitZMQPortUtilMap() {
        if (!_zmqPortUtilMap) {
            _zmqPortUtilMap = util::ZMQPortUtil::InitPortsConfig(*_properties);
        }
        return _zmqPortUtilMap;
    }

    std::unique_ptr<consensus::BlockOrderInterface> ModuleFactory::newGlobalBlockOrdering(std::function<bool(int chainId, int blockNumber)> deliverCallback) {
        // we reuse the rpc port as the global broadcast port
        auto portMap = getOrInitZMQPortUtilMap();
        if (portMap == nullptr) {
            return nullptr;
        }
        auto np = _properties->getNodeProperties();
        auto localNode = np.getLocalNodeInfo();
        auto localRegionNodes = np.getGroupNodesInfo(localNode->groupId);
        auto localReceiverPorts = portMap->at(localNode->groupId)[localNode->nodeId]->getLocalServicePorts(util::PortType::LOCAL_BLOCK_ORDER);
        auto [localReceivers, suc1] = util::ZMQPortUtil::WrapPortWithConfig(localRegionNodes, localReceiverPorts);
        if (!suc1) {
            return nullptr;
        }

        std::vector<std::shared_ptr<util::NodeConfig>> multiRaftParticipantNodes;
        std::vector<int> multiRaftParticipantPorts;
        std::vector<int> multiRaftLeaderPos;
        for (int i=0; i<(int)portMap->size(); i++) {
            auto reserveCount = ((int)portMap->at(i).size() - 1) / 3 + 1;  // at least f+1 receivers
            auto raftNodes = np.getGroupNodesInfo(i);
            auto raftPorts = portMap->at(i)[0]->getLocalServicePorts(util::PortType::CFT_PEER_TO_PEER);
            multiRaftLeaderPos.push_back((int)multiRaftParticipantNodes.size());    // the first node in a region is a leader
            for (int j=0; j<reserveCount; j++) {
                multiRaftParticipantNodes.push_back(raftNodes[j]);
                multiRaftParticipantPorts.push_back(raftPorts[j]);
            }
        }
        // build the zmq port config
        auto [multiRaftParticipant, suc2] = util::ZMQPortUtil::WrapPortWithConfig(multiRaftParticipantNodes, multiRaftParticipantPorts);
        if (!suc2) {
            return nullptr;
        }
        auto [bccsp, tp] = getOrInitBCCSPAndThreadPool();
        auto callback = BlockOrderType::NewRaftCallback(getOrInitContentStorage(), std::move(bccsp), std::move(tp));
        callback->setOnExecuteBlockCallback(std::move(deliverCallback));
        return BlockOrderType::NewBlockOrder(localReceivers, multiRaftParticipant, multiRaftLeaderPos, localNode, std::move(callback));
    }

    bool ModuleFactory::startReplicatorSender() {
        auto nodeProperties = _properties->getNodeProperties();
        auto localNode = nodeProperties.getLocalNodeInfo();
        auto initialBlockHeight = _properties->getStartBlockNumber(localNode->groupId);
        return _replicator->startSender(initialBlockHeight);
    }

    std::shared_ptr<::peer::BlockLRUCache> ModuleFactory::initUserRPCController() {
        auto gc = _properties->getNodeProperties().getGroupCount();
        if (gc <= 0) {
            return nullptr;
        }
        auto storage = std::make_shared<peer::BlockLRUCache>(gc);

        auto portMap = getOrInitZMQPortUtilMap();
        auto np = _properties->getNodeProperties();
        auto localNode = np.getLocalNodeInfo();
        auto& nodePortCfg = portMap->at(localNode->groupId)[localNode->nodeId];
        if (!::peer::core::UserRPCController::NewRPCController(
                storage,
                nodePortCfg->getLocalServicePorts(util::PortType::BFT_RPC)[localNode->nodeId])) {
            return nullptr;
        }
        return storage;
    }

    BFTController::~BFTController() = default;
}
