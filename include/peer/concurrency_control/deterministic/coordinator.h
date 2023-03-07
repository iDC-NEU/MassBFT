//
// Created by peng on 2/21/23.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "bthread/countdown_event.h"
#include "peer/db/rocksdb_connection.h"
#include "reserve_table.h"

namespace peer::cc {
    template<class WorkerFSMType, class ReserveTableType, class Derived>
    class Coordinator {
    public:
        static std::unique_ptr<Derived> NewCoordinator(const std::shared_ptr<peer::db::RocksdbConnection>& dbc, int workerCount) {
            std::unique_ptr<Derived> ptr(new Derived());
            auto* c = static_cast<Coordinator*>(ptr.get());
            auto table = std::make_shared<ReserveTableType>();
            c->reserveTable = table;
            c->workerList.reserve(workerCount);
            c->fsmList.reserve(workerCount);
            for (int i=0; i<workerCount; i++) {
                auto fsm = std::make_shared<WorkerFSMType>();
                if (fsm == nullptr) {
                    LOG(ERROR) << "Create fsm failed!";
                    return nullptr;
                }
                fsm->setDB(dbc);
                fsm->setReserveTable(table);
                auto worker = peer::cc::Worker::NewWorker(fsm);
                if (worker == nullptr) {
                    LOG(ERROR) << "Create worker failed!";
                    return nullptr;
                }
                worker->setId(i);
                auto ret = worker->checkAndStartService();
                if (!ret) {
                    LOG(ERROR) << "Start worker failed!";
                    return nullptr;
                }
                c->fsmList.push_back(std::move(fsm));
                c->workerList.push_back(std::move(worker));
            }
            return ptr;
        }

        virtual ~Coordinator() = default;

        Coordinator(const Coordinator&) = delete;

        Coordinator(Coordinator&&) = delete;

        // NOT thread safe, CAN NOT be called consecutively!
        bool invokeCustomCommand(std::function<void(const Worker&, WorkerFSMType&)>& command) {
            auto ret = processParallel(InvokerCommand::CUSTOM, ReceiverState::FINISH_CUSTOM, command);
            if (!ret) {
                LOG(ERROR) << "Command execution failed!";
                return false;
            }
            return true;
        }

    protected:
        Coordinator() = default;

        // InvokerCommand: worker fsm input
        // ReceiverState: worker fsm output
        // Block until all worker finish
        bool processParallel(InvokerCommand inputCommand, ReceiverState expectRetState,
                             // before a worker finish, called by Worker
                             const std::function<void(const Worker&, WorkerFSMType&)>& beforeFinish) {
            countdown.reset((int)workerList.size());
            bool ret = true;    // no need to be atomic
            DCHECK(workerList.size() == fsmList.size());
            for (int i=0; i<(int)fsmList.size(); i++) {
                const auto& worker = workerList[i];
                worker->setCommandCallback([&, i=i](ReceiverState rs){
                    if (expectRetState != rs) {
                        ret = false;    // fsm error, system must shutdown!
                    }
                    if (beforeFinish) {
                        beforeFinish(*workerList[i], *fsmList[i]);
                    }
                    countdown.signal();
                });
                // spin up worker
                worker->execute(inputCommand);
            }
            // wait worker result
            countdown.wait();
            return ret;
        }

    protected:
        std::vector<std::unique_ptr<Worker>> workerList;
        std::vector<std::shared_ptr<WorkerFSMType>> fsmList;
        std::shared_ptr<ReserveTableType> reserveTable;
        // countdown: for method processParallel
        bthread::CountdownEvent countdown;
    };

}