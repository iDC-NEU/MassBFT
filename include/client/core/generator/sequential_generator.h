//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include "glog/logging.h"
#include <atomic>
#include <memory>

namespace client::core {
    /**
     * Generates a sequence of integers between A and B
     */
    class SequentialGenerator : public NumberGenerator {
    private:
        std::atomic<uint64_t> counter{};
        uint64_t interval, countStart;

    public:
        static auto NewSequentialGenerator(uint64_t countStart, uint64_t countEnd) {
            return std::make_unique<SequentialGenerator>(countStart, countEnd);
        }

        /**
       * Create a counter that starts at countstart.
       */
        SequentialGenerator(long countStart, long countEnd)
                : interval(countEnd - countStart + 1), countStart(countStart) {
            setLastValue(counter.load());
        }

        uint64_t nextValue() override {
            auto ret = countStart + counter++ % interval;
            setLastValue(ret);
            return ret;
        }

        uint64_t lastValue() override {
            return counter.load() + 1;
        }

        double mean() override {
            CHECK(false) << "Can't compute mean of non-stationary distribution!";
            return -1;
        }
    };
}
