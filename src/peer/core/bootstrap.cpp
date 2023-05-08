//
// Created by user on 23-5-8.
//

#include "peer/core/bootstrap.h"
#include "peer/core/node_info_helper.h"
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
}
