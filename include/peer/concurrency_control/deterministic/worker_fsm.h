//
// Created by peng on 2/20/23.
//

#pragma once

#include "bthread/butex.h"

#include "glog/logging.h"

#include <memory>
#include <functional>
#include <thread>

namespace peer::cc {

    // for invoker
    enum class InvokerCommand {
        IDLE = 0,       // default state
        START = 1,      // initialization
        EXEC = 2,       // start the execution stage of the protocol
        COMMIT = 3,     // start the validation stage of the protocol
        EXIT = 4,       // exit the worker
    };

    // for worker
    enum class ReceiverState {
        READY = 0,          // worker is initializing or is free
        FINISH_EXEC = 1,    // worker finish execution phase
        FINISH_COMMIT = 2,  // worker finish validate phase
        EXITED = 3,         // worker is about to exit, use join func to safe delete it.
    };

    class WorkerFSM {
    public:
        virtual ~WorkerFSM() = default;

        WorkerFSM() = default;

        WorkerFSM(const WorkerFSM&) = delete;

        WorkerFSM(WorkerFSM&&) = delete;

        virtual ReceiverState OnCreate() = 0;

        virtual ReceiverState OnDestroy() = 0;

        virtual ReceiverState OnExecuteTransaction() = 0;

        virtual ReceiverState OnCommitTransaction() = 0;

    };

    class Worker {
    public:
        static std::unique_ptr<Worker> NewWorker(std::shared_ptr<WorkerFSM> fsm) {
            if (!fsm) {
                LOG(ERROR) << "WorkerFSM pointer is nil!";
                return nullptr;
            }
            std::unique_ptr<Worker> worker(new Worker());
            worker->_fsm = std::move(fsm);
            return worker;
        }

        virtual ~Worker() {
            _commandCallback = nullptr;
            execute(InvokerCommand::EXIT);
            if (_tid) { _tid->join(); }
            bthread::butex_destroy(_command);
        }

        Worker(const Worker &) = delete;

        Worker(Worker &&) = delete;

        // set a command to worker, response is in callback
        int execute(InvokerCommand command) {
            _command->store(static_cast<int>(command), std::memory_order_relaxed);
            return bthread::butex_wake_all(_command);
        }

        void setCommandCallback(std::function<void(ReceiverState)> commandCallback) {
            _commandCallback = std::move(commandCallback);
        }

        bool checkAndStartService() {
            if (!_fsm) {
                LOG(ERROR) << "WorkerFSM pointer is nil!";
                return false;
            }
            execute(InvokerCommand::START);
            _tid = std::make_unique<std::thread>(&Worker::run, this);
            return true;
        }

        inline void setId(int id_) { id = id_; }

        [[nodiscard]] inline int getId() const { return id; }

    protected:
        void run() {
            int cEnum = static_cast<int>(InvokerCommand::IDLE);
            do {
                while(_command->compare_exchange_strong(cEnum, cEnum, std::memory_order_acquire, std::memory_order_relaxed)) {
                    bthread::butex_wait(_command, cEnum, nullptr);
                }
                ReceiverState ret = ReceiverState::READY;  // return value of command
                switch (static_cast<InvokerCommand>(cEnum)) {
                    case InvokerCommand::IDLE:
                        continue;   // goto the front, retry;
                    case InvokerCommand::START:
                        ret = _fsm->OnCreate();
                        break;
                    case InvokerCommand::EXEC:
                        ret = _fsm->OnExecuteTransaction();
                        break;
                    case InvokerCommand::COMMIT:
                        ret = _fsm->OnCommitTransaction();
                        break;
                    case InvokerCommand::EXIT:
                        ret = _fsm->OnDestroy();
                        break;
                }
                if (_commandCallback) {
                    _commandCallback(ret);
                }
            } while (static_cast<InvokerCommand>(cEnum) != InvokerCommand::EXIT);
        }

    protected:
        Worker() { _command = bthread::butex_create_checked<butil::atomic<int>>(); }

    private:
        int id = -1; // the worker id
        std::unique_ptr<std::thread> _tid;
        std::function<void(ReceiverState)> _commandCallback;
        // the fsm may be shared among workers
        std::shared_ptr<WorkerFSM> _fsm;
        butil::atomic<int> *_command;
    };

}