#pragma once

#include "peer/concurrency_control/coordinator.h"
#include "peer/concurrency_control/worker_fsm.h"
#include "peer/chaincode/chaincode.h"
#include "proto/transaction.h"

namespace peer::cc::serial {
    class SerialWorkerFSM : public WorkerFSM {
        ReceiverState OnDestroy() override {
            return peer::cc::ReceiverState::EXITED;
        }

        ReceiverState OnCreate() override {
            return peer::cc::ReceiverState::READY;
        }

        ReceiverState OnExecuteTransaction() override {
            return peer::cc::ReceiverState::FINISH_EXEC;
        }

        ReceiverState OnCommitTransaction() override {
            return peer::cc::ReceiverState::FINISH_COMMIT;
        }
    };

    class SerialCoordinator : public Coordinator<SerialWorkerFSM, SerialCoordinator> {
    public:
        using TxnListType = std::vector<std::unique_ptr<proto::Transaction>>;
        using ResultType = proto::Transaction::ExecutionResult;

    public:
        bool init(const std::shared_ptr<peer::db::DBConnection>& dbc) {
            db = dbc;
            return true;
        }

        bool processValidatedRequests(std::vector<std::unique_ptr<proto::Envelop>>& requests,
                                      std::vector<std::unique_ptr<proto::TxReadWriteSet>>& retRWSets,
                                      std::vector<std::byte>& retResults) override {
            retResults.resize(requests.size());
            retRWSets.resize(requests.size());

            // 1 prepare txn function
            auto& fsmTxnList = _txnList;
            fsmTxnList.clear();
            fsmTxnList.reserve(requests.size());
            for (auto& it: requests) {
                auto txn = proto::Transaction::NewTransactionFromEnvelop(std::move(it));
                DCHECK(txn != nullptr) << "Can not get exn from envelop!";
                fsmTxnList.push_back(std::move(txn));   // txn may be nullptr
            }
            // 2 exec txn
            for (auto& txn: fsmTxnList) {
                // find the chaincode using ccList
                auto ccNameSV = txn->getUserRequest().getCCNameSV();
                auto* chaincode = createOrGetChaincode(ccNameSV);
                auto& userRequest = txn->getUserRequest();
                auto ret = chaincode->InvokeChaincode(userRequest.getFuncNameSV(), userRequest.getArgs());
                // get the rwSets out of the orm
                txn->setRetValue(chaincode->reset(txn->getReads(), txn->getWrites()));
                // 1. transaction internal error, abort it without adding reserve table
                if (ret != 0) {
                    txn->setExecutionResult(ResultType::ABORT_NO_RETRY);
                    continue;
                }
                // for committed transactions, read only optimization
                if (txn->getWrites().empty()) {
                    txn->setExecutionResult(ResultType::COMMIT);
                    continue;
                }
                // apply the write set and continue
                auto saveToDBFunc = [&](auto* batch) {
                    txn->setExecutionResult(ResultType::COMMIT);
                    for (const auto& kv: txn->getWrites()) {
                        auto& keySV = kv->getKeySV();
                        auto& valueSV = kv->getValueSV();
                        if (valueSV.empty()) {
                            batch->Delete({keySV.data(), keySV.size()});
                        } else {
                            batch->Put({keySV.data(), keySV.size()}, {valueSV.data(), valueSV.size()});
                        }
                    }
                    return true;
                };
                if (!db->syncWriteBatch(saveToDBFunc)) {
                    LOG(ERROR) << "WorkerFSMImpl can not write to db!";
                }
            }
            // 3 finish exec txn
            for (int i = 0; i < (int)requests.size(); i += 1) {
                auto& txn = fsmTxnList[i];
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
            return true;
        }

        bool processSync(const auto&, const auto&) {
            CHECK(false);
            return true;
        }

        friend class Coordinator;

    protected:
        SerialCoordinator() = default;

        inline peer::chaincode::Chaincode* createOrGetChaincode(std::string_view ccNameSV) {
            auto it = ccList.find(ccNameSV);
            if (it == ccList.end()) {   // chaincode not found
                auto orm = peer::chaincode::ORM::NewORMFromDBInterface(db);
                auto ret = peer::chaincode::NewChaincodeByName(ccNameSV, std::move(orm));
                CHECK(ret != nullptr) << "chaincode name not exist!";
                auto& rawPointer = *ret;
                ccList[ccNameSV] = std::move(ret);
                return &rawPointer;
            } else {
                return it->second.get();
            }
        }

    private:
        TxnListType _txnList;
        util::MyFlatHashMap<std::string, std::unique_ptr<peer::chaincode::Chaincode>> ccList;
        std::shared_ptr<db::DBConnection> db;
    };


}