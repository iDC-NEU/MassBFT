//
// Created by user on 23-3-7.
//

#pragma once

#include "peer/concurrency_control/deterministic/coordinator.h"
#include "peer/concurrency_control/deterministic/write_based/wb_worker_fsm.h"

namespace peer::cc {
    class WBCoordinator : public Coordinator<WBWorkerFSM, WBReserveTable, WBCoordinator> {
        bool processSync(const auto& afterStart, const auto& afterCommit) {
            reserveTable->reset();
            auto ret = processParallel(InvokerCommand::START, ReceiverState::READY, afterStart);
            if (!ret) {
                LOG(ERROR) << "init txnList failed!";
                return false;
            }
            ret = processParallel(InvokerCommand::EXEC, ReceiverState::FINISH_EXEC, nullptr);
            if (!ret) {
                LOG(ERROR) << "exec txnList failed!";
                return false;
            }
            // First commit, WBCoordinator add this function
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, nullptr);
            if (!ret) {
                LOG(ERROR) << "first commit failed!";
                return false;
            }
            // Second commit
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, afterCommit);
            if (!ret) {
                LOG(ERROR) << "second commit failed!";
                return false;
            }
            return true;
        }

        friend class Coordinator;

    protected:
        WBCoordinator() = default;

    private:

    };
}