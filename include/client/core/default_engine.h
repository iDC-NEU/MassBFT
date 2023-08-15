//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/status_thread.h"
#include "client/core/client_thread.h"
#include "client/core/workload.h"
#include "client/tpcc/tpcc_property.h"

namespace client::core {
    template <typename Derived>
    concept useDerivedDBFactory = requires(const util::Properties &n) {
        { Derived::CreateDBFactory(n) } -> std::same_as<std::unique_ptr<core::DBFactory>>;
    };

    template <class Derived, class PropertyType>
    requires requires(Derived d, const util::Properties &n) {
        { d.CreateWorkload(n) } -> std::same_as<std::shared_ptr<core::Workload>>;
        { d.CreateProperty(n) } -> std::same_as<std::unique_ptr<PropertyType>>;
    }
    class DefaultEngine {
    public:
        explicit DefaultEngine(const util::Properties &n) {
            if constexpr (useDerivedDBFactory<Derived>) {
                factory = Derived::CreateDBFactory(n);
            } else {
                factory = std::make_unique<core::DBFactory>(n);
            }
            workload = Derived::CreateWorkload(n);
            workload->init(n);
            measurements = std::make_shared<core::Measurements>();
            workload->setMeasurements(measurements);
            properties = Derived::CreateProperty(n);
            initClients();
        }

        ~DefaultEngine() { waitUntilFinish(); }

        // not thread safe, called by ths same manager
        void startTest() {
            startTestNoWait();
            waitUntilFinish();
        }

        // not thread safe, called by ths same manager
        void startTestNoWait() {
            auto warmupSeconds = properties->getWarmupSeconds();
            auto benchmarkSeconds = properties->getBenchmarkSeconds();
            LOG(INFO) << "Running test.";
            auto status = factory->newDBStatus();
            statusThread = std::make_unique<core::StatusThread>(measurements, std::move(status), warmupSeconds);
            LOG(INFO) << "Run worker thread";
            for(auto &client :clients) {
                client->run();
            }
            LOG(INFO) << "Run status thread";
            statusThread->run();
            benchmarkUntil = std::chrono::system_clock::now()
                    + std::chrono::seconds(warmupSeconds + benchmarkSeconds)
                    + std::chrono::milliseconds(100);
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
            auto operationCount = properties->getTargetThroughput() * properties->getBenchmarkSeconds();
            auto threadCount = std::min(properties->getThreadCount(), (int)operationCount);
            auto threadOpCount = std::max(static_cast<double>(operationCount) / threadCount, 1.0);
            auto tpsPerThread = static_cast<double>(properties->getTargetThroughput()) / threadCount;
            // use static seed, seed MUST start from 1 (seed=0 and seed=1 may generate the same sequence)
            unsigned long seed = 1;
            if (properties->getUseRandomSeed()) {
                // Generate a random seed
                seed = util::Timer::time_now_ns();
            }
            // Randomize seed of this thread
            core::GetThreadLocalRandomGenerator()->seed(seed++);
            for (int tid = 0; tid < threadCount; tid++) {   // create a set of clients
                auto db = factory->newDB();  // each client create a connection
                // Randomize seed of client thread
                auto t = std::make_unique<core::ClientThread>(std::move(db), workload, seed++, (int)threadOpCount, tpsPerThread);
                clients.emplace_back(std::move(t));
            }
        }

    private:
        std::unique_ptr<core::DBFactory> factory;
        std::chrono::high_resolution_clock::time_point benchmarkUntil;
        std::unique_ptr<PropertyType> properties;
        std::shared_ptr<core::Workload> workload;
        std::shared_ptr<core::Measurements> measurements;
        std::unique_ptr<core::StatusThread> statusThread;
        std::vector<std::unique_ptr<core::ClientThread>> clients;
    };
}