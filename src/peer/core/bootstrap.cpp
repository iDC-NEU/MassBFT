//
// Created by user on 23-5-8.
//

#include "peer/core/bootstrap.h"
#include "peer/core/node_info_helper.h"
#include "peer/core/yaml_key_storage.h"
#include "peer/replicator/replicator.h"

#include "ca/bft_instance_controller.h"

#include "common/property.h"

namespace peer::core {
    ModuleFactory::~ModuleFactory() = default;

    std::unique_ptr<ModuleFactory> ModuleFactory::NewModuleFactory(const std::shared_ptr<util::Properties>& properties) {
        std::unique_ptr<ModuleFactory> mf(new ModuleFactory);
        mf->_properties = properties;
        auto nodeInfoHelper = NodeInfoHelper::NewNodeInfoHelper(properties);
        if (nodeInfoHelper == nullptr) {
            return nullptr;
        }
        mf->_nodeInfoHelper = std::move(nodeInfoHelper);
        return mf;
    }

    std::shared_ptr<::util::BCCSP> ModuleFactory::getOrInitBCCSP() {
        if (_bccsp) {
            return _bccsp;
        }
        auto node = _properties->getCustomProperties("bccsp");
        _bccsp = std::make_shared<util::BCCSP>(std::make_unique<YAMLKeyStorage>(node));
        return _bccsp;
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
        if (_bccsp == nullptr || _contentStorage == nullptr) {
            return nullptr;   // not set
        }
        auto np = _properties->getNodeProperties();
        std::unordered_map<int, std::vector<util::NodeConfigPtr>> nodes;
        for (int i=0; i<np.getGroupCount(); i++) {
            // Cross-domain block transmission needs to transmit the erasure code
            // segment to the corresponding remote node, so a public network address is required
            nodes[i] = np.getGroupNodesInfo(i);
        }
        // ---- init port map
        std::unordered_map<int, int> regionNodesCount;
        for (const auto& it: nodes) {
            regionNodesCount[it.first] = (int)it.second.size();
        }
        auto zmqPortUtilMap = util::ZMQPortUtil::InitPortsConfig(_properties->replicatorLowestPort(), regionNodesCount, _properties->isDistributedSetting());
        // ---- TODO: extract the method
        auto localNode = np.getLocalNodeInfo();
        if (localNode == nullptr) {
            return nullptr;
        }
        auto replicator = std::make_shared<peer::Replicator>(nodes, localNode);
        replicator->setBCCSP(_bccsp);
        replicator->setStorage(_contentStorage);
        replicator->setPortUtilMap(zmqPortUtilMap);
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
        auto localNode = _properties->getNodeProperties().getLocalNodeInfo();
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
        // ports config of local region
//        ::peer::v2::ZMQPortUtilList portsConfig;
//        for (int i=0; i<4; i++) {
//            portsConfig.emplace_back(new peer::v2::SingleServerZMQPortUtil(regionServerCount, 0, i, offset));
//        }
//        std::vector<NodeHostConfig>
//        // todo: multi pbft
//        auto ic = ca::BFTInstanceController::NewBFTInstanceController(
//                sshConfig,
//                groupId,
//                localNode->nodeId,
//                runningPath,
//                _properties->getJVMPath());
//        return ic;
return nullptr;
    }
}
