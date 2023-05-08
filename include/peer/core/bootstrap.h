//
// Created by user on 23-5-8.
//

#pragma once

#include <memory>

namespace util {
    class Properties;
    class BCCSP;
}

namespace peer {
    class Replicator;
    namespace core {
        class NodeInfoHelper;
    }
}

namespace peer::core {
    class ModuleFactory {
    protected:
        ModuleFactory() = default;

    public:
        static std::unique_ptr<ModuleFactory> NewModuleFactory(const std::shared_ptr<util::Properties>& properties);

        virtual ~ModuleFactory();

        std::unique_ptr<::peer::Replicator> newReplicator();

        std::shared_ptr<::util::BCCSP> getBCCSP();

    private:
        std::shared_ptr<util::Properties> _properties;
        std::unique_ptr<NodeInfoHelper> _nodeInfoHelper;

    private:
        std::shared_ptr<::util::BCCSP> _bccsp;
    };
}