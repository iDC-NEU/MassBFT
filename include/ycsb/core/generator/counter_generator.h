//
// Created by peng on 10/18/22.
//

#pragma once

#include <atomic>
#include <memory>
#include "glog/logging.h"
#include "generator.h"

namespace ycsb::core {
    /**
     * Generates a sequence of integers.
     * (0, 1, ...)
     */
    class CounterGenerator : public NumberGenerator {
    public:
        static auto NewCounterGenerator(uint64_t startWith) {
            return std::make_unique<CounterGenerator>(startWith);
        }
        /**
         * Create a counter that starts at countstart.
         */
        explicit CounterGenerator(uint64_t startWith)
                : i(startWith) { }
        uint64_t nextValue() override {
            return i++;
        }
        double mean() override {
            CHECK(false) << "Can't compute mean of non-stationary distribution!";
            return -1;
        }
    private:
        std::atomic<uint64_t> i;
    };
}
