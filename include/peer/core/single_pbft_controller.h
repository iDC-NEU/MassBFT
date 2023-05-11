//
// Created by user on 23-5-10.
//

#pragma once

#include "ca/bft_instance_controller.h"
#include "peer/consensus/block_content/local_pbft_controller.h"

namespace peer::core {
    class SinglePBFTController {
    public:
        SinglePBFTController(std::unique_ptr<::ca::BFTInstanceController> instanceManager,
                             std::unique_ptr<::peer::consensus::LocalPBFTController> consensusHandler,
                             int nodeGroupId,
                             int instanceId,
                             int bftGroupId)
                : _ce(1), _nodeGroupId(nodeGroupId), _instanceId(instanceId), _bftGroupId(bftGroupId),
                  _instanceManager(std::move(instanceManager)), _consensusHandler(std::move(consensusHandler)) {
            CHECK(_instanceManager && _consensusHandler);
            _statusThread = std::make_unique<std::thread>(&SinglePBFTController::run, this);
        }

        ~SinglePBFTController() {
            _running = false;
            _statusThread->join();
        }

        [[nodiscard]] ::peer::consensus::LocalPBFTController& getConsensusHandler() const { return *_consensusHandler; }

        void run() {
            std::stringbuf buf;
            bool isInit = true;
            while(_running) {
                std::ostream out(&buf);
                auto success = _instanceManager->getChannelResponse(&out, &out);
                if (!success) {
                    LOG(WARNING) << "SinglePBFTController get log error!";
                    continue;
                }
                if (isInit) {
                    out.flush();
                    auto log = buf.str();
                    if (::ca::BFTInstanceController::IsInstanceReady(log)) {
                        isInit = false;
                        _ce.signal();
                    }
                }
            }
            auto fileName = "bft_log_" + std::to_string(_nodeGroupId) + "_"
                            + std::to_string(_instanceId) + "_"
                            + std::to_string(_bftGroupId) + ".txt";
            std::ofstream outputFile(fileName);
            outputFile << &buf;
            outputFile.close();
        }

        void waitUntilReady() { _ce.wait(); }

    private:
        std::atomic<bool> _running = true;
        bthread::CountdownEvent _ce;
        const int _nodeGroupId;
        const int _instanceId;
        const int _bftGroupId;
        std::unique_ptr<std::thread> _statusThread;
        std::unique_ptr<::ca::BFTInstanceController> _instanceManager;
        std::unique_ptr<::peer::consensus::LocalPBFTController> _consensusHandler;
    };
}