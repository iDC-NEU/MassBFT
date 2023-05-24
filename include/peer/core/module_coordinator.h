//
// Created by user on 23-5-16.
//

#pragma once

#include <memory>
#include <atomic>
#include <thread>
#include "common/async_serial_executor.h"

namespace util {
    class Properties;
    class NodeConfig;
}

namespace peer {
    class MRBlockStorage;
    namespace consensus::v2 {
        class BlockOrder;
    }
    namespace cc {
        class CoordinatorImpl;
    }
    namespace db {
        class RocksdbConnection;
    }
}

namespace peer::core {
    class ModuleFactory;
    class SinglePBFTController;

    class ModuleCoordinator {
    public:
        static std::unique_ptr<ModuleCoordinator> NewModuleCoordinator(const std::shared_ptr<util::Properties>& properties);

        ~ModuleCoordinator();

        ModuleCoordinator(const ModuleCoordinator&) = delete;

        ModuleCoordinator(ModuleCoordinator&&) noexcept = delete;

        [[nodiscard]] auto& getModuleFactory() const { return *_moduleFactory; }

        void startInstance();

        void waitInstanceReady() const;

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
        std::unique_ptr<::peer::consensus::v2::BlockOrder> _gbo;
        std::unique_ptr<::peer::core::SinglePBFTController> _localContentBFT;
        // for debug
        std::shared_ptr<util::NodeConfig> _localNode;
        // for concurrency control
        std::shared_ptr<peer::db::RocksdbConnection> _db;
        std::unique_ptr<peer::cc::CoordinatorImpl> _cc;
        util::AsyncSerialExecutor _serialExecutor;
    };
}