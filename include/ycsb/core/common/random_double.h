//
// Created by peng on 10/17/22.
//

#pragma once

#include <memory>
#include <random>
#include <memory>
#include "glog/logging.h"
#include "ycsb/core/generator/generator.h"
#include "common/thread_local_store.h"

namespace ycsb::utils {
    class RandomDouble : public core::DoubleGenerator {
    public:
        static auto NewRandomDouble(uint64_t seed) {
            return std::make_unique<RandomDouble>(seed);
        }

        explicit RandomDouble(uint64_t seed, double min = 0.0, double max = 1.0)
                : generator(*util::ThreadLocalStore<std::default_random_engine>::Get()), uniform(min, max) {
            generator.seed(seed);
        }
        // This function does not support concurrent entry.
        double nextValue() override {
            return uniform(generator);
        }
        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }
    private:
        std::default_random_engine& generator;
        std::uniform_real_distribution<double> uniform;
    };
}
