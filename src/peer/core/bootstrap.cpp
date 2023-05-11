//
// Created by user on 23-5-8.
//

#include "peer/core/bootstrap.h"
#include "peer/core/yaml_key_storage.h"
#include "peer/replicator/replicator.h"
#include "peer/consensus/block_content/local_pbft_controller.h"
#include "ca/bft_instance_controller.h"
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
        _bccsp = std::make_shared<util::BCCSP>(std::make_unique<YAMLKeyStorage>(node));
        _threadPoolForBCCSP = std::make_shared<util::thread_pool_light>();
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
        if (!_contentStorage) {
            _contentStorage = std::make_shared<peer::MRBlockStorage>(gc);
        }
        return _contentStorage;
    }

    std::shared_ptr<::peer::Replicator> ModuleFactory::getOrInitReplicator() {
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
        auto replicator = std::make_shared<peer::Replicator>(nodes, localNode);
        auto [bccsp, tp] = getOrInitBCCSPAndThreadPool();
        if (!bccsp || !tp) {
            return nullptr;
        }
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

    std::shared_ptr<::ca::BFTInstanceController> ModuleFactory::newReplicatorBFTController(int groupId) {
        auto np = _properties->getNodeProperties();
        auto localNode = np.getLocalNodeInfo();
        auto [user, pass, success] = _properties->getSSHInfo();
        if (!success) {
            LOG(WARNING) << "please check your ssh setting in config file.";
        }
        auto runningPath = std::filesystem::current_path();
        ca::SSHConfig sshConfig {
                .ip = localNode->priIp,
                .port = -1,
                .userName = user,
                .password = pass,
        };
        auto ic = ca::BFTInstanceController::NewBFTInstanceController(
                sshConfig,
                groupId,
                localNode->nodeId,
                runningPath,
                _properties->getJVMPath());
        // generate host file
        auto portMap = getOrInitZMQPortUtilMap();
        if (!portMap) {
            return nullptr;
        }
        std::vector<ca::NodeHostConfig> hostList;
        auto& groupPortMap = portMap->at(localNode->groupId);
        auto localRegionNodes = np.getGroupNodesInfo(localNode->groupId);
        CHECK(localRegionNodes.size() == portMap->at(localNode->groupId).size());
        for (int i=0; i<(int)localRegionNodes.size(); i++) {
            auto& node = localRegionNodes[i];
            hostList.push_back({
                                       .processId = node->nodeId,
                                       .ip = node->priIp,
                                       .serverToServerPort = groupPortMap[i]->getServerToServerPorts()[i],
                                       .serverToClientPort = groupPortMap[i]->getClientToServerPorts()[i],
                                       .rpcPort =  groupPortMap[i]->getBFTRpcPorts()[i],
                               });
        }
        ic->prepareConfigurationFile(hostList);
        // ----- init LocalPBFTController ----
        auto [bccsp, tp] = getOrInitBCCSPAndThreadPool();
        if (!bccsp || !tp) {
            return nullptr;
        }
        auto cs = getOrInitContentStorage();
        if (!cs) {
            return nullptr;
        }
        // local region nodes
        auto controller = consensus::LocalPBFTController::NewPBFTController(
                localRegionNodes,
                localNode->nodeId,
                groupPortMap.at(localNode->nodeId),
                bccsp,
                std::move(tp),
                std::move(cs),
                {_properties->getBlockBatchTimeoutMs(),
                 _properties->getBlockMaxBatchSize()},
                 _properties->validateOnReceive());
        if (!controller) {
            return nullptr;
        }
        // todo: merge controller and ic together

        return nullptr;
    }

    std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> ModuleFactory::getOrInitZMQPortUtilMap() {
        if (_zmqPortUtilMap) {
            return _zmqPortUtilMap;
        }
        auto np = _properties->getNodeProperties();
        std::unordered_map<int, int> regionNodesCount;
        for (int i=0; i<np.getGroupCount(); i++) {
            regionNodesCount[i] = np.getGroupNodeCount(i);
        }
        _zmqPortUtilMap = util::ZMQPortUtil::InitPortsConfig(_properties->replicatorLowestPort(), regionNodesCount, _properties->isDistributedSetting());
        return _zmqPortUtilMap;
    }
}
