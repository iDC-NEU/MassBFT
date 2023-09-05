//
// Created by peng on 2/21/23.
//

#pragma once

#include "peer/concurrency_control/crdt/crdt_worker_fsm.h"
#include "peer/db/db_interface.h"
#include "bthread/countdown_event.h"

namespace peer::cc::crdt {
    class CRDTCoordinator : public Coordinator<CRDTWorkerFSM, CRDTCoordinator> {
    public:
        bool init(const std::shared_ptr<peer::db::DBConnection>& dbc) {
            auto dbShim = std::make_shared<peer::crdt::chaincode::DBShim>(dbc);
            auto table = std::make_shared<ReserveTable>();
            for (auto& it: this->fsmList) {
                it->setDBShim(dbShim);
            }
            return true;
        }

        bool processSync(const auto& afterStart, const auto& afterCommit) {
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
        CRDTCoordinator() = default;
    };
}