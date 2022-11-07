//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_WORKLOAD_H
#define BENCHMARKCLIENT_WORKLOAD_H

#include <string>
#include <atomic>
#include "ycsb/core/common/exception.h"
#include "yaml-cpp/yaml.h"

namespace ycsb::core {
    class DB;
    class Status;
    namespace workload {
        /**
        * One experiment scenario. One object of this type will
        * be instantiated and shared among all client threads. This class
        * should be constructed using a no-argument constructor, so we can
        * load it dynamically. Any argument-based initialization should be
        * done by init().
        *
        * If you extend this class, you should support the "insertstart" property. This
        * allows the Client to proceed from multiple clients on different machines, in case
        * the client is the bottleneck. For example, if we want to load 1 million records from
        * 2 machines, the first machine should have insertstart=0 and the second insertstart=500000. Additionally,
        * the "insertcount" property, which is interpreted by Client, can be used to tell each instance of the
        * client how many inserts to do. In the example above, both clients should have insertcount=500000.
        */
        class Workload {
        public:
            constexpr static const auto INSERT_START_PROPERTY = "insertstart";
            constexpr static const auto INSERT_COUNT_PROPERTY = "insertcount";
            constexpr static const auto INSERT_START_PROPERTY_DEFAULT = 0;
            /** Operations available for a database. */
            enum class WorkloadOperation {
                READ,
                UPDATE,
                INSERT,
                SCAN,
                DELETE,
                };
        private:
            std::atomic<bool> stopRequested = false;
        public:
            virtual ~Workload() = default;
            /**
            * Initialize the scenario. Create any generators and other shared objects here.
            * Called once, in the main client thread, before any operations are started.
            */
            virtual void init(const YAML::Node& n) noexcept(false) = 0;
            /**
            * Initialize any state for a particular client thread. Since the scenario object
            * will be shared among all threads, this is the place to create any state that is specific
            * to one thread. To be clear, this means the returned object should be created anew on each
            * call to initThread(); do not return the same object multiple times.
            * The returned object will be passed to invocations of doInsert() and doTransaction()
            * for this thread. There should be no side effects from this call; all state should be encapsulated
            * in the returned object. If you have no state to retain for this thread, return null. (But if you have
            * no state to retain for this thread, probably you don't need to override initThread().)
            *
            * @return false if the workload knows it is done for this thread. Client will terminate the thread.
            * Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read
            * traces from a file, return true when there are more to do, false when you are done.
            */
            virtual void* initThread(const YAML::Node& node, int myThreadID, int threadCount) noexcept(false) {
                return nullptr;
            }
            /**
            * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
            */
            virtual void cleanup() noexcept(false) {}
            /**
            * Do one insert operation. Because it will be called concurrently from multiple client threads, this
            * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
            * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
            * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
            * synchronized, since each thread has its own threadstate instance.
            */
            virtual bool doInsert(DB* db, void* threadState) = 0;
            /**
            * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
            * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
            * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
            * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
            * synchronized, since each thread has its own threadstate instance.
            *
            * @return false if the workload knows it is done for this thread. Client will terminate the thread.
            * Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read
            * traces from a file, return true when there are more to do, false when you are done.
            */
            virtual bool doTransaction(DB* db, void* threadState) = 0;
            /**
            * Allows scheduling a request to stop the workload.
            */
            void requestStop() {
                stopRequested.store(true, std::memory_order_relaxed);
            }

            /**
            * Check the status of the stop request flag.
            * @return true if stop was requested, false otherwise.
            */
            bool isStopRequested() {
                return stopRequested.load(std::memory_order_relaxed);
            }
        };
    }
}
#endif //BENCHMARKCLIENT_WORKLOAD_H
