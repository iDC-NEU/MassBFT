//
// Created by user on 23-5-10.
//

#pragma once

#include "peer/consensus/pbft/bft_instance_controller.h"
#include "bthread/countdown_event.h"
#include <thread>

namespace peer::consensus::v2 {
    class SinglePBFTController {
    public:
        SinglePBFTController(std::unique_ptr<::peer::consensus::BFTInstanceController> instanceManager,
                             int nodeGroupId,
                             int instanceId,
                             int bftGroupId)
                : _ce(1), _nodeGroupId(nodeGroupId), _instanceId(instanceId), _bftGroupId(bftGroupId),
                  _instanceManager(std::move(instanceManager)) {
            _instanceManager->stopAndClean();
        }

        ~SinglePBFTController() {
            _running = false;
            if (_statusThread) {
                _statusThread->join();
            }
            _instanceManager->stopAndClean();
        }

        void startInstance() {
            _instanceManager->startInstance("");    // we have already prepared the file
            _statusThread = std::make_unique<std::thread>(&SinglePBFTController::run, this);
        }

        void waitUntilReady() { _ce.wait(); }

    protected:
        void run() {
            pthread_setname_np(pthread_self(), "bft_ctl");
            std::stringbuf buf;
            while(_running) {
                auto ret = _instanceManager->isInstanceReady(10 * 1000);
                if (ret == std::nullopt) {
                    LOG(ERROR) << "BFT instance is crashed, return.";
                    break;  // channel is close
                }
                if (*ret == true) {
                    _ce.signal();
                    break;
                }
                LOG(INFO) << "Waiting for bft instance to be ready.";
            }
            auto fileName = "bft_log_" + std::to_string(_nodeGroupId) + "_"
                            + std::to_string(_instanceId) + "_"
                            + std::to_string(_bftGroupId) + ".txt";
            std::ofstream outputFile(fileName);
            outputFile << &buf;
            outputFile.close();
        }

    private:
        std::atomic<bool> _running = true;
        bthread::CountdownEvent _ce;
        const int _nodeGroupId;
        const int _instanceId;
        const int _bftGroupId;
        std::unique_ptr<std::thread> _statusThread{};
        std::unique_ptr<::peer::consensus::BFTInstanceController> _instanceManager;
    };
}