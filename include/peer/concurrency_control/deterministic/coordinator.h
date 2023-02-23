//
// Created by peng on 2/21/23.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm_impl.h"
#include "bthread/countdown_event.h"

namespace peer::cc {
    class Coordinator {
    public:
        static std::unique_ptr<Coordinator> NewCoordinator(const std::shared_ptr<peer::db::LeveldbConnection>& dbc, int workerCount) {
            std::unique_ptr<Coordinator> c(new Coordinator());
            auto table = std::make_shared<peer::cc::ReserveTable>();
            c->reserveTable = table;
            c->workerList.reserve(workerCount);
            c->fsmList.reserve(workerCount);
            for (int i=0; i<workerCount; i++) {
                auto fsm = std::make_shared<WorkerFSMImpl>();
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
            return c;
        }

        ~Coordinator() = default;

        Coordinator(const Coordinator&) = delete;

        Coordinator(Coordinator&&) = delete;

        // NOT thread safe, CAN NOT be called consecutively!
        bool invokeCustomCommand(std::function<void(const Worker&, WorkerFSMImpl&)>& command) {
            auto ret = processParallel(InvokerCommand::CUSTOM, ReceiverState::FINISH_CUSTOM, command);
            if (!ret) {
                LOG(ERROR) << "Command execution failed!";
                return false;
            }
            return true;
        }

        // NOT thread safe
        // processTxnList will take the transactions and move them back after execution
        bool processTxnList(std::vector<std::unique_ptr<proto::Transaction>>& txnList) {
            int totalWorkerCount = (int)workerList.size();
            reserveTable->reset();
            // prepare txn function
            auto afterStart = [&](const Worker& worker, WorkerFSMImpl& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                fsmTxnList.clear();
                auto id = worker.getId();
                for (int i = id; i < (int)txnList.size(); i += totalWorkerCount) {
                    fsmTxnList.push_back(std::move(txnList[i]));
                }
            };
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
            // move back
            auto afterCommit = [&](const Worker& worker, WorkerFSMImpl& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                auto id = worker.getId();
                for (int i = id, j = 0; i < (int)txnList.size(); i += totalWorkerCount) {
                    txnList[i] = std::move(fsmTxnList[j++]);
                }
            };
            ret = processParallel(InvokerCommand::COMMIT, ReceiverState::FINISH_COMMIT, afterCommit);
            if (!ret) {
                LOG(ERROR) << "commit txnList failed!";
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
                             const std::function<void(const Worker&, WorkerFSMImpl&)>& beforeFinish) {
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

    private:
        std::vector<std::unique_ptr<Worker>> workerList;
        std::vector<std::shared_ptr<WorkerFSMImpl>> fsmList;
        std::shared_ptr<ReserveTable> reserveTable;
        // countdown: for method processParallel
        bthread::CountdownEvent countdown;
    };

}