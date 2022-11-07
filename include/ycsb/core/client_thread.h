//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_CLIENT_THREAD_H
#define NEUCHAIN_PLUS_CLIENT_THREAD_H

#include <thread>
#include "common/concurrent_queue/light_weight_semaphore.h"
#include "db.h"
#include "ycsb/core/workload/workload.h"
#include "common/timer.h"

namespace ycsb::core {
/**
 * A thread for executing transactions or data inserts to the database.
 */
    class ClientThread {
    private:
        // Counts down each of the clients completing.
        moodycamel::LightweightSemaphore &completeLatch;
        bool spinSleep;
        std::unique_ptr<DB> db;
        bool doTransactions;
        workload::Workload* workload;
        int opCount;
        double targetOpsPerMs;
        int opsDone;
        int tid;
        int threadCount;
        const YAML::Node& props;
        uint64_t targetOpsTickNs;
    /**
     * Constructor.
     *
     * @param db                   the DB implementation to use
     * @param dotransactions       true to do transactions, false to insert data
     * @param workload             the workload to use
     * @param props                the properties defining the experiment
     * @param opcount              the number of operations (transactions or inserts) to do
     * @param targetperthreadperms target number of operations per thread per ms
     * @param completeLatch        The latch tracking the completion of all clients.
     */
    public:
        ClientThread(std::unique_ptr<DB> db, bool doTransactions, workload::Workload* workload, const YAML::Node& props, int opCount,
                     double targetPerThreadPerms, moodycamel::LightweightSemaphore& completeLatch)
                     :completeLatch(completeLatch), db(std::move(db)), doTransactions(doTransactions),
                     workload(workload), opCount(opCount), opsDone(0), props(props) {
            if (targetPerThreadPerms > 0) {
                targetOpsPerMs = targetPerThreadPerms;
                targetOpsTickNs = (uint64_t) (1000000 / targetOpsPerMs);
            }
            spinSleep = props["spin.sleep"].as<bool>(false);
            tid = 0;
            threadCount = 0;
        }
        inline void setThreadId(int id) {
            this->tid = id;
        }
        inline void setThreadCount(int tc) {
            threadCount = tc;
        }

        [[nodiscard]] inline int getOpsDone() const {
            return opsDone;
        }

        // return a running thread instance
        inline auto run() {
            DCHECK(tid != 0);
            DCHECK(threadCount != 0);
            return std::make_unique<std::thread>(&ClientThread::doWork, this);
        }

        /**
         * The total amount of work this thread is still expected to do.
         */
        [[nodiscard]] inline int getOpsTodo() const {
            auto todo = opCount - opsDone;
            return todo < 0 ? 0 : todo;
        }

        ClientThread(const ClientThread&) = delete;


    private:
        void doWork() {
            db->init();
            auto workloadState = workload->initThread(props, tid, threadCount);
            // TODO: destroy workloadState
            if (workloadState == nullptr) {
                CHECK(false) << "init workload failed, tid: " << tid;
            }
            //NOTE: Switching to using nanoTime and parkNanos for time management here such that the measurements
            // and the client thread have the same view on time.

            // spread the thread operations out, so they don't all hit the DB at the same time
            // GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
            // and the sleep() doesn't make sense for granularities < 1 ms anyway
            if ((targetOpsPerMs > 0) && (targetOpsPerMs <= 1.0)) {
                auto randGen = utils::RandomUINT64::NewRandomUINT64(tid); // use tid as seed, the delay is unique
                auto randomMinorDelay = randGen->nextValue() % targetOpsTickNs;
                util::Timer::sleep_ns((long)randomMinorDelay);
            }
            try {
                auto startTimeNanos = util::Timer::time_now_ns();
                if (doTransactions) {
                    // opCount == 0 inf ops
                    while (((opCount == 0) || (opsDone < opCount)) && !workload->isStopRequested()) {

                        if (!workload->doTransaction(db.get(), workloadState)) {
                            LOG(ERROR) << "Do transaction failed, opsDone: " << opsDone;
                            break;
                        }
                        opsDone++;
                        throttleNanos(startTimeNanos);
                    }
                } else {
                    while (((opCount == 0) || (opsDone < opCount)) && !workload->isStopRequested()) {
                        if (!workload->doInsert(db.get(), workloadState)) {
                            LOG(ERROR) << "Do insert failed, opsDone: " << opsDone;
                            break;
                        }
                        opsDone++;
                        throttleNanos(startTimeNanos);
                    }
                }
                db->cleanup();
            } catch (...) {
                LOG(ERROR) << "Exception caught, opsDone: " << opsDone;
            }
            completeLatch.signal();
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
