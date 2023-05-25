//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_WORKLOAD_H
#define BENCHMARKCLIENT_WORKLOAD_H

#include <string>
#include <atomic>
#include "ycsb/core/common/exception.h"
#include "ycsb/core/common/ycsb_property.h"
#include "yaml-cpp/yaml.h"

namespace ycsb::core {
    class DB;
    namespace workload {
        /**
        * One experiment scenario. One object of this type will
        * be instantiated and shared among all client threads. This class
        * should be constructed using a no-argument constructor, so we can
        * load it dynamically. Any argument-based initialization should be
        * done by init().
        */
        class Workload {
        public:
            virtual ~Workload() = default;

            virtual void init(const utils::YCSBProperties& n) = 0;

            virtual bool doInsert(DB* db) const = 0;

            virtual bool doTransaction(DB* db) const = 0;

            void requestStop() { stopRequested.store(true, std::memory_order_relaxed); }

            [[nodiscard]] bool isStopRequested() const { return stopRequested.load(std::memory_order_relaxed); }

        private:
            std::atomic<bool> stopRequested = false;
        };
    }
}
#endif //BENCHMARKCLIENT_WORKLOAD_H
