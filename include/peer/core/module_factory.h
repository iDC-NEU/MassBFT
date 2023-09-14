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
    namespace consensus::v2 {
        class BlockOrder;
        class LocalConsensusController;
        class SinglePBFTController;
    }
    class MRBlockStorage;
    class BlockLRUCache;
    class Replicator;
    namespace direct {
        class Replicator;
    }
}

namespace peer::core {
    struct BFTController {
        ~BFTController();
        std::unique_ptr<consensus::v2::LocalConsensusController> consensusController;
        std::unique_ptr<consensus::v2::SinglePBFTController> pbftController;
    };

    class ModuleFactory {
    protected:
        ModuleFactory() = default;

    public:
        using ReplicatorType = peer::Replicator;
        // using ReplicatorType = peer::direct::Replicator;

        static std::unique_ptr<ModuleFactory> NewModuleFactory(const std::shared_ptr<util::Properties>& properties);

        virtual ~ModuleFactory();

        // Called after PBFT consensuses a block
        std::shared_ptr<ReplicatorType> getOrInitReplicator();

        bool startReplicatorSender();

        std::shared_ptr<::peer::BlockLRUCache> initUserRPCController();

        // this function never return nil value
        std::pair<std::shared_ptr<::util::BCCSP>, std::shared_ptr<::util::thread_pool_light>> getOrInitBCCSPAndThreadPool();

        std::shared_ptr<::peer::MRBlockStorage> getOrInitContentStorage();

        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> getOrInitZMQPortUtilMap();

        // groupId: the bft group id (not region id!)
        // bft instance runningPath = std::filesystem::current_path();
        std::unique_ptr<BFTController> newReplicatorBFTController(int groupId);

        std::unique_ptr<::peer::consensus::v2::BlockOrder> newGlobalBlockOrdering(std::function<bool(int chainId, int blockNumber)> deliverCallback);

    private:
        std::shared_ptr<util::Properties> _properties;

    private:
        std::shared_ptr<::util::BCCSP> _bccsp;
        std::shared_ptr<::util::thread_pool_light> _threadPoolForBCCSP;
        std::shared_ptr<::peer::MRBlockStorage> _contentStorage;
        std::shared_ptr<ReplicatorType> _replicator;
        std::shared_ptr<std::unordered_map<int, util::ZMQPortUtilList>> _zmqPortUtilMap;
    };
}