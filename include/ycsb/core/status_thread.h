//
// Created by peng on 11/6/22.
//

#pragma once

#include "common/concurrent_queue/light_weight_semaphore.h"
#include "client_thread.h"
#include "ycsb/core/measurements/measurements.h"
#include <vector>
#include <memory>

namespace ycsb::core {
/**
 * A thread to periodically show the status of the experiment to reassure you that progress is being made.
 */
    class StatusThread {
    public:
        using ClientPairList = std::vector<std::pair<std::unique_ptr<ClientThread>, std::unique_ptr<std::thread>>>;
    private:
        // Counts down each of the clients completing.
        moodycamel::LightweightSemaphore &completeLatch;

        // Stores the measurements for the run
        Measurements *measurements;

        // The clients that are running.
        ClientPairList clients;

        const std::string label;
        bool standardStatus{};

        // The interval for reporting status.
        time_t sleepTimeNs{};


        /**
         * Creates a new StatusThread without JVM stat tracking.
         *
         * @param completeLatch         The latch that each client thread will {@link CountDownLatch#countDown()}
         *                              as they complete.
         * @param clients               The clients to collect metrics from.
         * @param label                 The label for the status.
         * @param standardstatus        If true the status is printed to stdout in addition to stderr.
         * @param statusIntervalSeconds The number of seconds between status updates.
         */
    public:
        StatusThread(moodycamel::LightweightSemaphore &completeLatch, ClientPairList clients,
                     const auto &label, bool standardStatus, int statusIntervalSeconds)
                : completeLatch(completeLatch), clients(std::move(clients)), label(label),
                  standardStatus(standardStatus), sleepTimeNs(statusIntervalSeconds * 1000000000) {
            measurements = Measurements::getMeasurements();
        }

        /**
         * Run and periodically report status.
         */
    public:
        void run() {

        }

        /**
         * Waits for all of the client to finish or the deadline to expire.
         *
         * @param deadline The current deadline.
         * @return True if all of the clients completed.
         */
        bool waitForClientsUntil(double deadline) {
            auto timer = util::Timer();
            // wait for all threads is complete
            for (auto i = clients.size();
                 i > 0; i -= completeLatch.waitMany((int) i, time_t((deadline - timer.end())) * 1000000)) {
                if (deadline - timer.end() < 0) {
                    // time up
                    return false;
                }
            }
            return true;
        }
    };
}
