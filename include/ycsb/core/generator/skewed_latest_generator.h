//
// Created by peng on 10/18/22.
//

#pragma once

#include "ycsb/core/generator/zipfian_generator.h"
#include "ycsb/core/generator/counter_generator.h"

namespace ycsb::core {

    class SkewedLatestGenerator : public NumberGenerator {
        CounterGenerator* basis;
        std::unique_ptr<ZipfianGenerator> zipfian;
    public:
        static auto NewSkewedLatestGenerator(uint64_t seed, CounterGenerator* basis) {
            return std::make_unique<SkewedLatestGenerator>(seed, basis);
        }

        SkewedLatestGenerator(uint64_t seed, CounterGenerator* basis) {
            this->basis = basis;
            zipfian = ZipfianGenerator::NewZipfianGenerator(seed, 0, this->basis->lastValue()-1);
            SkewedLatestGenerator::nextValue();
        }

        /**
         * Generate the next string in the distribution, skewed Zipfian favoring the items most recently returned by
         * the basis generator.
         */
        uint64_t nextValue() override {
            uint64_t max = basis->lastValue();
            uint64_t next = max - zipfian->nextLong(max);
            setLastValue(next);
            return next;
        }

        double mean() override {
            CHECK(false) << "Can't compute mean of non-stationary distribution!";
            return -1;
        }
    };

}
