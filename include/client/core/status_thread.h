//
// Created by peng on 11/6/22.
//

#pragma once

#include "client/core/measurements.h"
#include "client/core/db.h"
#include <vector>
#include <memory>

namespace client::core {
    class StatusThread {
    public:
        StatusThread(std::shared_ptr<Measurements> m, std::unique_ptr<DBStatus> dbStatus, int warmupSeconds)
                : measurements(std::move(m)), dbStatus(std::move(dbStatus)), warmupSeconds(warmupSeconds) { }

        ~StatusThread() {
            running = false;
            if (_statusThread) {
                _statusThread->join();
            }
            if (_monitorThread) {
                _monitorThread->join();
            }
        }

        inline void run() {
            _statusThread = std::make_unique<std::thread>(&StatusThread::doStatus, this);
            _monitorThread = std::make_unique<std::thread>(&StatusThread::doMonitor, this);
        }

    protected:
        void doStatus() {
            pthread_setname_np(pthread_self(), "ycsb_print");
            util::Timer timer;
            size_t lastTimeCommit = 0;
            size_t lastTimeAbort = 0;
            size_t lastTimePending = 0;
            auto sleepUntil = std::chrono::system_clock::now() + std::chrono::seconds(1);
            bool warmedUp = false;
            auto warmedUpTime = std::chrono::system_clock::now() + std::chrono::milliseconds(warmupSeconds * 1000);

            while(running.load(std::memory_order_relaxed)) {
                if (!warmedUp && sleepUntil > warmedUpTime) {
                    warmedUp = true;
                    timer.start();
                    LOG(INFO) << "The system warmup is completed, and the statistical indicators will be reset.";
                    txCountCommit = 0;
                    txCountAbort = 0;
                    latencySum = 0;
                    lastTimeCommit = 0;
                    lastTimeAbort = 0;
                }
                std::this_thread::sleep_until(sleepUntil);
                sleepUntil = std::chrono::system_clock::now() + std::chrono::seconds(1);
                auto currentSecCommit = txCountCommit - lastTimeCommit;
                auto currentSecAbort = txCountAbort - lastTimeAbort;
                auto pendingTxnSize = measurements->getPendingTransactionCount();
                auto currentTimePending = pendingTxnSize - lastTimePending;
                LOG(INFO) << "In the last 1s, commit: " << currentSecCommit
                          << ", abort: " << currentSecAbort
                          << ", send rate: " << currentSecCommit + currentSecAbort + currentTimePending
                          << ", latency_ms: " << latencySum / std::max(txCountCommit, uint64_t(1))
                          << ", pendingTx: " << pendingTxnSize;
                lastTimeCommit = txCountCommit;
                lastTimeAbort = txCountAbort;
                lastTimePending = pendingTxnSize;
            }
            LOG(INFO) << "Detail summary:";
            LOG(INFO) << "# Transaction throughput (KTPS): " << (double) txCountCommit / timer.end() / 1000;
            LOG(INFO) << "  Abort rate (KTPS): " << (double) txCountAbort / timer.end() / 1000;
            LOG(INFO) << "  Send rate (KTPS): " << static_cast<double>(txCountCommit + txCountAbort + measurements->getPendingTransactionCount()) / timer.end() / 1000;
            LOG(INFO) << "Avg committed latency: " << latencySum / std::max(txCountCommit, uint64_t(1)) << " ms.";
        }

        void doMonitor() {
            pthread_setname_np(pthread_self(), "ycsb_monitor");
            if (!dbStatus->getTop(blockHeight, 5, 1000)) {
                LOG(WARNING) << "Failed to obtain block height, start from 0.";
                blockHeight = 0;
            }
            blockHeight += 1;   // The next block.
            while(running.load(std::memory_order_relaxed)) {
                int64_t timeMsWhenReturn = 0;
                std::unique_ptr<proto::Block> block = dbStatus->getLightBlock(blockHeight, timeMsWhenReturn);
                if (block == nullptr) {
                    continue;
                }
                auto txnCount = block->body.userRequests.size();
                auto latencyList  = measurements->getTxnLatency(*block, timeMsWhenReturn);
                auto& filterList = block->executeResult.transactionFilter;
                CHECK(txnCount == latencyList.size());
                CHECK(txnCount == filterList.size());
                DLOG(INFO) << "polled blockHeight: " << blockHeight << ", size: " << txnCount;
                // calculate txCountCommit, txCountAbort, latency
                for (int i=0; i<(int)txnCount; i++) {
                    if (latencyList[i] == 0) {
                        continue;
                    }
                    // We only calculate the latency of committed txn.
                    if (!static_cast<bool>(filterList[i])) {
                        txCountAbort += 1;
                        continue;
                    }
                    txCountCommit += 1;
                    latencySum += latencyList[i];
                }
                blockHeight++;
            }
        }

    private:
        std::atomic<bool> running = true;
        const std::shared_ptr<Measurements> measurements;
        const std::unique_ptr<DBStatus> dbStatus;
        const int warmupSeconds;

        int blockHeight = 0;
        uint64_t txCountCommit = 0;
        uint64_t txCountAbort = 0;
        uint64_t latencySum = 0;

        std::unique_ptr<std::thread> _statusThread;
        std::unique_ptr<std::thread> _monitorThread;
    };
}
