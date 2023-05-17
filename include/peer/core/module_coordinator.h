//
// Created by user on 23-5-16.
//

#pragma once

#include <memory>
#include <atomic>
#include <thread>

namespace util {
    class Properties;
}

namespace peer {
    class MRBlockStorage;
    namespace consensus::v2 {
        class BlockOrder;
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
    };
}