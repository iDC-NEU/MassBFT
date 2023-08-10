//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/common/exception.h"
#include "client/core/measurements.h"
#include "common/property.h"
#include <atomic>

namespace client::core {
    class DB;
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

        virtual void init(const ::util::Properties& n) = 0;

        virtual bool doInsert(DB* db) const = 0;

        virtual bool doTransaction(DB* db) const = 0;

        void requestStop() { stopRequested.store(true, std::memory_order_relaxed); }

        [[nodiscard]] bool isStopRequested() const { return stopRequested.load(std::memory_order_relaxed); }

        void setMeasurements(auto&& rhs) { measurements = std::forward<decltype(rhs)>(rhs); }

    private:
        std::atomic<bool> stopRequested = false;

    protected:
        mutable std::shared_ptr<Measurements> measurements;
    };
}
