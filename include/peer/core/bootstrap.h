//
// Created by user on 23-5-8.
//

#pragma once

#include "common/zmq_port_util.h"
#include <unordered_map>
#include <memory>

namespace util {
    class BCCSP;
}

namespace peer {
    class MRBlockStorage;
    class Replicator;
    namespace core {
        class NodeInfoHelper;
    }
}

namespace ca {
    class BFTInstanceController;
}

namespace peer::core {
    class ModuleFactory {
    protected:
        ModuleFactory() = default;

    public:
        static std::unique_ptr<ModuleFactory> NewModuleFactory(const std::shared_ptr<util::Properties>& properties);

        virtual ~ModuleFactory();

        // Called after PBFT consensuses a block
        std::shared_ptr<::peer::Replicator> getOrInitReplicator();

        std::shared_ptr<::util::BCCSP> getOrInitBCCSP();

        std::shared_ptr<::peer::MRBlockStorage> getOrInitContentStorage();

        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> getOrInitZMQPortUtilMap();

        std::shared_ptr<::ca::BFTInstanceController> newReplicatorBFTController(int groupId=0);

    private:
        std::shared_ptr<util::Properties> _properties;

    private:
        std::shared_ptr<::util::BCCSP> _bccsp;
        std::shared_ptr<::peer::MRBlockStorage> _contentStorage;
        std::shared_ptr<::peer::Replicator> _replicator;
        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> _zmqPortUtilMap;
    };
}