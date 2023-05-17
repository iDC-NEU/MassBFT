//
// Created by user on 23-5-8.
//

#pragma once

#include "common/zmq_port_util.h"
#include <unordered_map>
#include <memory>

namespace util {
    class BCCSP;
    class thread_pool_light;
}

namespace peer {
    namespace core {
        class SinglePBFTController;
    }
    namespace consensus::v2 {
        class BlockOrder;
        class OrderACB;
    }
    class MRBlockStorage;
    class Replicator;
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

        bool startReplicatorSender();

        std::pair<std::shared_ptr<::util::BCCSP>,
                std::shared_ptr<::util::thread_pool_light>> getOrInitBCCSPAndThreadPool();

        std::shared_ptr<::peer::MRBlockStorage> getOrInitContentStorage();

        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> getOrInitZMQPortUtilMap();

        // groupId: the bft group id (not region id!)
        // bft instance runningPath = std::filesystem::current_path();
        std::unique_ptr<::peer::core::SinglePBFTController> newReplicatorBFTController(int groupId);

        std::unique_ptr<::peer::consensus::v2::BlockOrder> newGlobalBlockOrdering(std::shared_ptr<peer::consensus::v2::OrderACB> callback);

    private:
        std::shared_ptr<util::Properties> _properties;

    private:
        std::shared_ptr<::util::BCCSP> _bccsp;
        std::shared_ptr<::util::thread_pool_light> _threadPoolForBCCSP;
        std::shared_ptr<::peer::MRBlockStorage> _contentStorage;
        std::shared_ptr<::peer::Replicator> _replicator;
        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> _zmqPortUtilMap;
    };
}