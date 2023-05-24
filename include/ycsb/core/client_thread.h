//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_CLIENT_THREAD_H
#define NEUCHAIN_PLUS_CLIENT_THREAD_H

#include <thread>
#include "lightweightsemaphore.h"
#include "db.h"
#include "ycsb/core/workload/workload.h"
#include "ycsb/core/workload/core_workload.h"
#include "common/timer.h"

namespace ycsb::core {
    class ClientThread {
    private:
        std::unique_ptr<DB> db;
        workload::Workload* workload;
        int seed = 0;
        int opCount;
        int opsDone;
        double targetOpsPerMs;
        uint64_t targetOpsTickNs;

    public:
        ClientThread(std::unique_ptr<DB> db,
                     workload::Workload* workload,
                     int opCount,
                     double tps)
                : db(std::move(db)), workload(workload), opCount(opCount), opsDone(0) {
            CHECK(tps > 0);
            targetOpsPerMs = tps * 1000;
            targetOpsTickNs = (uint64_t) (1000000 / targetOpsPerMs);
        }

        inline void setSeed(int id) { this->seed = id; }

        // return a running thread instance
        inline auto run() { return std::make_unique<std::thread>(&ClientThread::doWork, this); }

        [[nodiscard]] inline int getOpsTodo() const { return std::max(opCount - opsDone, 0); }

        [[nodiscard]] inline int getOpsDone() const { return opsDone; }

    private:
        void doWork() {
            utils::RandomUINT64::GetThreadLocalRandomGenerator()->seed(seed);
            if (targetOpsPerMs <= 1.0) {
                auto randGen = utils::RandomUINT64::NewRandomUINT64();
                auto randomMinorDelay = randGen->nextValue() % targetOpsTickNs;
                util::Timer::sleep_ns((long)randomMinorDelay);
            }
            auto startTimeNanos = util::Timer::time_now_ns();
            // opCount == 0 inf ops
            while ((opCount == 0 || opsDone < opCount) && !workload->isStopRequested()) {
                if (!workload->doTransaction(db.get())) {
                    LOG(ERROR) << "Do transaction failed, opsDone: " << opsDone;
                    break;
                }
                opsDone++;
                throttleNanos(startTimeNanos);
            }
        }

        inline void throttleNanos(time_t startTimeNanos) const {
            //throttle the operations
            if (targetOpsPerMs > 0) {
                // delay until next tick
                auto deadline = startTimeNanos + opsDone * targetOpsTickNs;
                util::Timer::sleep_ns((long)deadline - util::Timer::time_now_ns());
            }
        }
    };
}

#endif //NEUCHAIN_PLUS_CLIENT_THREAD_H
