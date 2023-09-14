//
// Created by user on 23-5-16.
//

#pragma once

#include <memory>
#include <atomic>
#include <thread>
#include "common/async_serial_executor.h"
#include "peer/db/db_interface.h"

namespace util {
    class Properties;
    struct NodeConfig;
}

namespace peer {
    class MRBlockStorage;
    class BlockLRUCache;
    namespace consensus {
        class BlockOrderInterface;
    }
    namespace cc {
        class CoordinatorImpl;
        namespace crdt {
            class CRDTCoordinator;
        }
    }
}

namespace peer::core {
    class ModuleFactory;
    struct BFTController;

    class ModuleCoordinator {
    public:
        // Uncomment this line to enable CRDT chaincode
        // using ChaincodeType = peer::cc::crdt::CRDTCoordinator;
        // Uncomment this line to enable traditional chaincode
        using ChaincodeType = peer::cc::CoordinatorImpl;

        static std::unique_ptr<ModuleCoordinator> NewModuleCoordinator(const std::shared_ptr<util::Properties>& properties);

        bool initChaincodeData(const std::string& ccName);

        bool initCrdtChaincodeData(const std::string& ccName);

        ~ModuleCoordinator();

        ModuleCoordinator(const ModuleCoordinator&) = delete;

        ModuleCoordinator(ModuleCoordinator&&) noexcept = delete;

        [[nodiscard]] auto& getModuleFactory() const { return *_moduleFactory; }

        bool startInstance();

        void waitInstanceReady() const;

        [[nodiscard]] std::shared_ptr<peer::db::DBConnection> getDBHandle() const { return _db; }

    protected:
        ModuleCoordinator() = default;

        void contentLeaderReceiverLoop();

        bool onConsensusBlockOrder(int regionId, int blockId);

    private:
        // for subscriber
        int _subscriberId = -1;
        std::atomic<bool> _running = true;
        std::unique_ptr<std::thread> _subscriberThread;
        // for moduleFactory
        std::unique_ptr<ModuleFactory> _moduleFactory;
        // other components
        std::shared_ptr<::peer::MRBlockStorage> _contentStorage;
        std::unique_ptr<::peer::consensus::BlockOrderInterface> _gbo;
        std::unique_ptr<BFTController> _localContentBFT;
        // for debug
        std::shared_ptr<util::NodeConfig> _localNode;
        // for concurrency control
        std::shared_ptr<peer::db::DBConnection> _db;
        std::unique_ptr<ChaincodeType> _cc;
        util::AsyncSerialExecutor _serialExecutor;
        // for user rpc
        std::shared_ptr<::peer::BlockLRUCache> _userRPCNotifier;
    };
}