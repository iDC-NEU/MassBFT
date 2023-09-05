//
// Created by peng on 2/21/23.
//

#pragma once

#include "peer/concurrency_control/crdt/crdt_worker_fsm.h"
#include "bthread/countdown_event.h"
#include "peer/db/db_interface.h"

namespace peer::cc::crdt {
    class CRDTCoordinator {
    public:
        static std::unique_ptr<CRDTCoordinator> NewCoordinator(const std::shared_ptr<peer::db::DBConnection>& dbc, int workerCount) {
            std::unique_ptr<CRDTCoordinator> ptr(new CRDTCoordinator());
            auto dbShim = std::make_shared<peer::crdt::chaincode::DBShim>(dbc);
            auto* c = ptr.get();
            c->workerList.reserve(workerCount);
            c->fsmList.reserve(workerCount);
            for (int i=0; i<workerCount; i++) {
                auto fsm = std::make_shared<CRDTWorkerFSM>();
                fsm->setDBShim(dbShim);
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

        virtual ~CRDTCoordinator() = default;

        CRDTCoordinator(const CRDTCoordinator&) = delete;

        CRDTCoordinator(CRDTCoordinator&&) = delete;

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

    protected:
        CRDTCoordinator() = default;

        // InvokerCommand: worker fsm input
        // ReceiverState: worker fsm output
        // Block until all worker finish
        bool processParallel(InvokerCommand inputCommand, ReceiverState expectRetState,
                // before a worker finish, called by Worker
                             const std::function<void(const Worker&, CRDTWorkerFSM&)>& beforeFinish) {
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
        std::vector<std::shared_ptr<CRDTWorkerFSM>> fsmList;
        // countdown: for method processParallel
        bthread::CountdownEvent countdown;
    };
}