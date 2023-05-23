//
// Created by peng on 11/6/22.
//

#pragma once

#include "lightweightsemaphore.h"
#include "client_thread.h"
#include "ycsb/core/measurements/measurements.h"
#include <vector>
#include <memory>
#include "fmt/format.h"

using fmt::format;

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
            const long startTimeMs = util::Timer::time_now_ms();
            const long startTimeNanos = util::Timer::time_now_ns();
            long deadline = startTimeNanos + sleepTimeNs;
            long startIntervalMs = startTimeMs;
            long lastTotalOps = 0;

            bool allDone;

            do {
                long nowMs = util::Timer::time_now_ms();

                lastTotalOps = computeStats(startTimeMs, startIntervalMs, nowMs, lastTotalOps);

                allDone = waitForClientsUntil(double(deadline));

                startIntervalMs = nowMs;
                deadline += sleepTimeNs;
            }
            while (!allDone);

            // Print the final stats.
            computeStats(startTimeMs, startIntervalMs, util::Timer::time_now_ms(), lastTotalOps);
        }

        /**
       * Computes and prints the stats.
       *
       * @param startTimeMs     The start time of the test.
       * @param startIntervalMs The start time of this interval.
       * @param endIntervalMs   The end time (now) for the interval.
       * @param lastTotalOps    The last total operations count.
       * @return The current operation count.
       */
    private:
            long computeStats(const long startTimeMs, long startIntervalMs, long endIntervalMs,
                              long lastTotalOps) {
            // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

            long totalOps = 0;
            long todoOps = 0;

            // Calculate the total number of operations completed.
            for (auto &t : clients) {
                totalOps += t.first->getOpsDone();
                todoOps += t.first->getOpsTodo();
            }

            long interval = endIntervalMs - startTimeMs;
            double throughput = 1000.0 * (((double) totalOps) / (double) interval);
            double curThroughput = 1000.0 * (((double) (totalOps - lastTotalOps)) /
                                             ((double) (endIntervalMs - startIntervalMs)));
            long estRemaining = (long) ceil(todoOps / throughput);

            time_t now = time(nullptr);
            std::string labelString = this->label + ctime(&now);


            std::string msg = labelString;
            msg = msg + " " + std::to_string(interval / 1000) + " sec: ";
            msg = msg + std::to_string(totalOps) + " operations; ";

            if (totalOps != 0) {
                msg.append(format("{:.2f}", curThroughput)).append(" current ops/sec; ");
            }
            if (todoOps != 0) {
                //TODO:: RemainingFormatter need to realize in client.h
                msg = msg + "est completion in " + std::to_string(estRemaining);
            }

            // msg.append(measurements->getMeasurements().);

            LOG(ERROR) << msg;

            if (standardStatus) {
                LOG(INFO) << msg;
            }
            return totalOps;
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
