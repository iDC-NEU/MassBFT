//
// Created by peng on 2/21/23.
//

#pragma once

#include "peer/concurrency_control/deterministic/worker_fsm.h"
#include "bthread/countdown_event.h"
#include "peer/db/db_interface.h"
#include "reserve_table.h"

namespace peer::cc {
    template<class WorkerFSMType, class ReserveTableType, class Derived>
    class Coordinator {
    public:
        static std::unique_ptr<Derived> NewCoordinator(const std::shared_ptr<peer::db::DBConnection>& dbc, int workerCount) {
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

        // NOT thread safe, processTxnList will take the transactions and move them back after execution
        bool processTxnList(std::vector<std::unique_ptr<proto::Transaction>>& txnList) {
            const auto totalWorkerCount = (int)workerList.size();
            // prepare txn function
            auto afterStart = [&](const auto& worker, auto& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                fsmTxnList.clear();
                auto id = worker.getId();
                for (int i = id; i < (int)txnList.size(); i += totalWorkerCount) {
                    fsmTxnList.push_back(std::move(txnList[i]));
                }
            };
            // move back
            auto afterCommit = [&](const auto& worker, auto& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                auto id = worker.getId();
                for (int i = id, j = 0; i < (int)txnList.size(); i += totalWorkerCount) {
                    txnList[i] = std::move(fsmTxnList[j++]);
                }
            };
            return static_cast<Derived*>(this)->processSync(afterStart, afterCommit);
        }

        bool processValidatedRequests(std::vector<std::unique_ptr<proto::Envelop>>& requests,
                                      std::vector<std::unique_ptr<proto::TxReadWriteSet>>& retRWSets,
                                      std::vector<std::byte>& retResults) {
            retResults.resize(requests.size());
            retRWSets.resize(requests.size());
            const auto totalWorkerCount = (int)workerList.size();
            // prepare txn function
            auto afterStart = [&](const auto& worker, auto& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                fsmTxnList.clear();
                fsmTxnList.reserve(requests.size() / totalWorkerCount + 1);
                for (int i = worker.getId(); i < (int)requests.size(); i += totalWorkerCount) {
                    auto txn = proto::Transaction::NewTransactionFromEnvelop(std::move(requests[i]));
                    CHECK(txn != nullptr) << "Can not get exn from envelop!";
                    fsmTxnList.push_back(std::move(txn));   // txn may be nullptr
                }
            };
            // move back
            auto afterCommit = [&](const auto& worker, auto& fsm) {
                auto& fsmTxnList = fsm.getMutableTxnList();
                auto id = worker.getId();
                for (int i = id, j = 0; i < (int)requests.size(); i += totalWorkerCount) {
                    auto& txn = fsmTxnList[j++];
                    retResults[i] = static_cast<std::byte>(false);
                    if (txn == nullptr) {
                        retRWSets[i] = std::make_unique<proto::TxReadWriteSet>();
                        continue;
                    }
                    if (txn->getExecutionResult() == proto::Transaction::ExecutionResult::COMMIT) {
                        retResults[i] = static_cast<std::byte>(true);
                    }
                    auto ret = proto::Transaction::DestroyTransaction(std::move(txn));
                    requests[i] = std::move(ret.first);
                    retRWSets[i] = std::move(ret.second);
                }
            };
            return static_cast<Derived*>(this)->processSync(afterStart, afterCommit);
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