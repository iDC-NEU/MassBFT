//
// Created by peng on 11/6/22.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/db.h"
#include <thread>

namespace client::core {
    class ClientThread {
    public:
        ClientThread(std::unique_ptr<DB> db,
                     std::shared_ptr<const Workload> workload,
                     int id,
                     int txnCount,
                     double txnPerSecond)
                : _db(std::move(db)), _workload(std::move(workload)), _seed(id), _txnCount(txnCount), _txnDone(0) {
            CHECK(txnPerSecond > 0);
            _txnPerMs = txnPerSecond / 1000.0;
            _txnTickNs = (int)(1000000 / _txnPerMs);
        }

        ~ClientThread() {
            _db->stop();
            if (_clientThread) {
                _clientThread->join();
            }
        }

        inline void run() { _clientThread = std::make_unique<std::thread>(&ClientThread::doWork, this); }

        [[nodiscard]] inline int getOpsTodo() const { return std::max(_txnCount - _txnDone, 0); }

        [[nodiscard]] inline int getOpsDone() const { return _txnDone; }

    protected:
        void doWork() {
            pthread_setname_np(pthread_self(), "ycsb_worker");
            DLOG(INFO) << "Worker send rate: " << _txnPerMs * 1000 << ", total: " << _txnCount;
            GetThreadLocalRandomGenerator()->seed(_seed);
            if (_txnPerMs <= 1.0) {
                auto randGen = utils::RandomUINT64::NewRandomUINT64();
                auto randomMinorDelay = randGen->nextValue() % _txnTickNs;
                std::this_thread::sleep_for(std::chrono::nanoseconds(randomMinorDelay));
            }
            auto deadline = std::chrono::system_clock::now() + std::chrono::nanoseconds(_txnTickNs);
            // opCount == 0 inf ops
            while ((_txnCount == 0 || _txnDone < _txnCount) && !_workload->isStopRequested()) {
                if (!_workload->doTransaction(_db.get())) {
                    LOG(ERROR) << "Do transaction failed, opsDone: " << _txnDone;
                    break;
                }
                _txnDone++;
                // delay until next tick
                deadline += std::chrono::nanoseconds(_txnTickNs);
                std::this_thread::sleep_until(deadline);
            }
            DLOG(INFO) << "Worker finished sending txn, opsDone: " << _txnDone;
        }

    private:
        std::unique_ptr<DB> _db;
        std::shared_ptr<const Workload> _workload;
        int _seed;
        int _txnCount;
        int _txnDone;
        double _txnPerMs;
        int _txnTickNs;
        std::unique_ptr<std::thread> _clientThread;
    };
}
