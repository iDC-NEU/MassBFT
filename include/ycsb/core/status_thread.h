//
// Created by peng on 11/6/22.
//

#pragma once

#include "ycsb/core/client_thread.h"
#include <vector>
#include <memory>

namespace ycsb::core {
    class StatusThread {
    private:
        std::atomic<bool> running;
        std::shared_ptr<Measurements> measurements;

        uint64_t blockHeight = 0;
        uint64_t txCountCommit = 0;
        uint64_t txCountAbort = 0;
        uint64_t latencySum = 0;
        uint64_t latencySampleCount = 1;

    public:
        explicit StatusThread(std::shared_ptr<Measurements> m) : measurements(std::move(m)) { }

        void runStatus() {
            pthread_setname_np(pthread_self(), "print_thread");
            util::Timer timer;
            size_t lastTimeCommit = 0;
            size_t lastTimeAbort = 0;
            size_t lastTimePending = 0;
            auto sleepUntil = std::chrono::system_clock::now() + std::chrono::seconds(1);

            while(running) {
                std::this_thread::sleep_until(sleepUntil);
                sleepUntil = std::chrono::system_clock::now() + std::chrono::seconds(1);
                auto currentSecCommit = txCountCommit - lastTimeCommit;
                auto currentSecAbort = txCountAbort - lastTimeAbort;
                auto pendingTxnSize = measurements->getPendingTransactionCount();
                auto currentTimePending = pendingTxnSize - lastTimePending;
                LOG(INFO) << "In the last 1s, commit+abort_no_retry: " << currentSecCommit
                          << ", abort: " << currentSecAbort
                          << ", send rate: " << currentSecCommit + currentSecAbort + currentTimePending
                          << ", latency: " << (double) latencySum / (double) latencySampleCount
                          << ", pendingTx: " << pendingTxnSize;
                lastTimeCommit = txCountCommit;
                lastTimeAbort = txCountAbort;
                lastTimePending = pendingTxnSize;
            }
            LOG(INFO) << "Detail summary:";
            LOG(INFO) << "# Transaction throughput (KTPS): " << (double) txCountCommit / timer.end() / 1000;
            LOG(INFO) << "  Abort rate (KTPS): " << (double) txCountAbort / timer.end() / 1000;
            LOG(INFO) << "  Send rate (KTPS): " << static_cast<double>(txCountCommit + txCountAbort + measurements->getPendingTransactionCount()) / timer.end() / 1000;
            LOG(INFO) << "Avg committed latency: " << (double) latencySum / (double) latencySampleCount << " sec.";
        }

        void runMonitor(const utils::YCSBProperties &n) {
            auto db = DB::NewDB("", n);  // each client create a connection
            pthread_setname_np(pthread_self(), "monitor_thread");
            while (running) {
                std::unique_ptr<proto::Block> block = db->getBlock((int)blockHeight);
                auto txnCount = block->body.userRequests.size();
                auto latencyList  = measurements->getTxnLatency(*block);
                auto& filterList = block->executeResult.transactionFilter;
                CHECK(txnCount == latencyList.size());
                CHECK(txnCount == filterList.size());
                LOG(INFO) << "polled blockHeight: " << blockHeight << ", size: " << txnCount;
                // calculate txCountCommit, txCountAbort, latency
                for (int i=0; i<(int)txnCount; i++) {
                    if (latencyList[i] == 0) {
                        DLOG(INFO) << "missing tx in tx map, please check if all txs is generated in a user.";
                        continue;
                    }
                    // We only calculate the latency of committed txn.
                    if (static_cast<bool>(filterList[i]) == false) {
                        txCountAbort += 1;
                        continue;
                    }
                    txCountCommit += 1;
                    latencySum += latencyList[i];
                    latencySampleCount += 1;
                }
                blockHeight++;
            }
        }
    };
}
