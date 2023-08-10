//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/status_thread.h"
#include "client/core/client_thread.h"
#include "client/ycsb/core_workload.h"
#include "client/ycsb/ycsb_property.h"

namespace client::ycsb {
    class YCSBEngine {
    public:
        explicit YCSBEngine(const util::Properties &n) :factory(n) {
            workload = std::make_shared<CoreWorkload>();
            workload->init(n);
            measurements = std::make_shared<core::Measurements>();
            workload->setMeasurements(measurements);
            ycsbProperties = YCSBProperties::NewFromProperty(n);
            initClients();
        }

        ~YCSBEngine() { waitUntilFinish(); }

        // not thread safe, called by ths same manager
        void startTest() {
            startTestNoWait();
            waitUntilFinish();
        }

        // not thread safe, called by ths same manager
        void startTestNoWait() {
            LOG(INFO) << "Running test.";
            auto status = factory.newDBStatus();
            statusThread = std::make_unique<core::StatusThread>(measurements, std::move(status));
            LOG(INFO) << "Run worker thread";
            for(auto &client :clients) {
                client->run();
            }
            LOG(INFO) << "Run status thread";
            statusThread->run();
            auto totalBenchmarkTime = static_cast<int>((double)ycsbProperties->getOperationCount() /
                                                       (ycsbProperties->getTargetTPSPerThread() * ycsbProperties->getThreadCount()));
            benchmarkUntil = std::chrono::system_clock::now() + std::chrono::seconds(totalBenchmarkTime);
        }

        // not thread safe, called by ths same manager
        void waitUntilFinish() {
            if (!statusThread) {
                return;
            }
            std::this_thread::sleep_until(benchmarkUntil);
            LOG(INFO) << "Finishing status thread";
            statusThread.reset();
            workload->requestStop();
            LOG(INFO) << "All worker exited";
        }

    protected:
        void initClients() {
            auto operationCount = ycsbProperties->getOperationCount();
            auto threadCount = std::min(ycsbProperties->getThreadCount(), (int)operationCount);
            auto threadOpCount = operationCount / threadCount;
            if (threadOpCount <= 0) {
                threadOpCount += 1;
            }
            auto tpsPerThread = ycsbProperties->getTargetTPSPerThread();
            // use static seed, seed MUST start from 1 (seed=0 and seed=1 may generate the same sequence)
            unsigned long seed = 1;
            if (ycsbProperties->getUseRandomSeed()) {
                // Generate a random seed
                seed = util::Timer::time_now_ns();
            }
            // Randomize seed of this thread
            core::GetThreadLocalRandomGenerator()->seed(seed++);
            for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
                auto db = factory.newDB();  // each client create a connection
                // Randomize seed of client thread
                auto t = std::make_unique<core::ClientThread>(std::move(db), workload, seed++, (int)threadOpCount, tpsPerThread);
                clients.emplace_back(std::move(t));
            }
        }

    private:
        core::DBFactory factory;
        std::chrono::high_resolution_clock::time_point benchmarkUntil;
        std::unique_ptr<YCSBProperties> ycsbProperties;
        std::shared_ptr<CoreWorkload> workload;
        std::shared_ptr<core::Measurements> measurements;
        std::unique_ptr<core::StatusThread> statusThread;
        std::vector<std::unique_ptr<core::ClientThread>> clients;
    };
}
