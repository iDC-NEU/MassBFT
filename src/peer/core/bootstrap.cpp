//
// Created by user on 23-5-8.
//

#include "peer/core/bootstrap.h"
#include "peer/core/node_info_helper.h"
#include "peer/core/yaml_key_storage.h"
#include "peer/replicator/replicator.h"
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

    std::shared_ptr<::util::BCCSP> ModuleFactory::getBCCSP() {
        if (!_bccsp) {
            auto node = _properties->getCustomProperties("bccsp");
            _bccsp = std::make_shared<util::BCCSP>(std::make_unique<YAMLKeyStorage>(node));
        }
        return _bccsp;
    }

    std::unique_ptr<::peer::Replicator> ModuleFactory::newReplicator() {
        auto np = _properties->getNodeProperties();
        std::unordered_map<int, std::vector<util::NodeConfigPtr>> nodes;
        for (int i=0; i<np.getGroupCount(); i++) {
            // Cross-domain block transmission needs to transmit the erasure code
            // segment to the corresponding remote node, so a public network address is required
            nodes[i] = np.getGroupNodesInfo(i);
        }

        return nullptr;
    }
}
