//
// Created by user on 23-3-7.
//

#pragma once

#include "peer/concurrency_control/coordinator.h"
#include "peer/concurrency_control/deterministic/worker_fsm_impl.h"

namespace peer::cc {
    class CoordinatorImpl : public Coordinator<WorkerFSMImpl, CoordinatorImpl> {
    public:
        bool init(const std::shared_ptr<peer::db::DBConnection>& dbc) {
            auto table = std::make_shared<ReserveTable>();
            this->reserveTable = table;
            for (auto& it: this->fsmList) {
                it->setDB(dbc);
                it->setReserveTable(table);
            }
            return true;
        }

        bool processSync(const auto& afterStart, const auto& afterCommit) {
            reserveTable->reset();
            // prepare txn function
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
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, afterCommit);
            if (!ret) {
                LOG(ERROR) << "commit txnList failed!";
                return false;
            }
            return true;
        }

        friend class Coordinator;

    protected:
        CoordinatorImpl() = default;

    private:
        std::shared_ptr<ReserveTable> reserveTable{};
    };
}