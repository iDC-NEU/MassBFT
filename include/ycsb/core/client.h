//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_CLIENT_H
#define BENCHMARKCLIENT_CLIENT_H

#include <vector>
#include <thread>
#include <string>
#include <cassert>
#include "lightweightsemaphore.h"
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"
#include "ycsb/core/workload/workload.h"
#include "ycsb/core/workload/core_workload.h"
#include "ycsb/core/db_factory.h"
#include "client_thread.h"

namespace ycsb::core {
    class Client {
    public:
        // Whether this is the transaction phase (run) or not (load).
        constexpr static const auto DO_TRANSACTIONS_PROPERTY = "dotransactions";
        // The target number of operations to perform.
        constexpr static const auto OPERATION_COUNT_PROPERTY = "operationcount";
        /**
         * Indicates how many inserts to do if less than recordcount.
         * Useful for partitioning the load among multiple servers if the client is the bottleneck.
         * Additional workloads should support the "insertstart" property which tells them which record to start at.
         */
        constexpr static const auto INSERT_COUNT_PROPERTY = "insertcount";

        // The number of records to load into the database initially.
        constexpr static const auto RECORD_COUNT_PROPERTY = "recordcount";

        constexpr static const auto DEFAULT_RECORD_COUNT = 0;

        constexpr static const auto THREAD_COUNT_PROPERTY = "threadcount";

        constexpr static const auto TARGET_PROPERTY = "target";

        constexpr static const auto LABEL_PROPERTY = "label";

        // Use a global workload to create some worker class
        // each worker instance is assigned with a unique db instance created by dbName
        static auto initDB(const std::string& dbName, const YAML::Node& n,
                                               int threadCount, double targetPerThreadPerms,
                                               workload::Workload* workload,
                                               moodycamel::LightweightSemaphore& completeLatch) {
            bool doTransaction = n[DO_TRANSACTIONS_PROPERTY].as<bool>(true);
            uint64_t opCount;
            if (doTransaction) {
                opCount = n[OPERATION_COUNT_PROPERTY].as<uint64_t>(0);
            } else {
                if (n[INSERT_COUNT_PROPERTY].IsDefined()) {
                    opCount = n[INSERT_COUNT_PROPERTY].as<uint64_t>(0);
                } else {
                    opCount = n[RECORD_COUNT_PROPERTY].as<uint64_t>(DEFAULT_RECORD_COUNT);
                }
            }
            if (threadCount > (int)opCount && opCount > 0) {
                threadCount = (int)opCount;
                LOG(WARNING) << "Warning: the threadcount is bigger than recordcount, the threadcount will be recordcount!";
            }
            std::vector<std::pair<std::unique_ptr<ClientThread>, std::unique_ptr<std::thread>>> clients;
            for (int tid = 0; tid < threadCount; tid++) {
                auto db = DBFactory::NewDB(dbName, n);
                auto threadOpCount = opCount / threadCount;
                // ensure correct number of operations, in case opcount is not a multiple of threadcount
                if (tid < (int)opCount % threadCount) {
                    ++threadOpCount;
                }
                auto t = std::make_unique<ClientThread>(std::move(db), doTransaction, workload, n, (int)threadOpCount, targetPerThreadPerms, completeLatch);
                t->setThreadId(tid);
                t->setThreadCount(threadCount);
                clients.emplace_back(std::move(t), t->run());
            }
            return clients;
        }
    };
}
#endif //BENCHMARKCLIENT_CLIENT_H
