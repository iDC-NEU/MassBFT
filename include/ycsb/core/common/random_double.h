//
// Created by peng on 10/17/22.
//

#pragma once

#include <memory>
#include <random>
#include <memory>
#include "glog/logging.h"
#include "ycsb/core/generator/generator.h"

namespace ycsb::utils {
    class RandomDouble : public core::DoubleGenerator {
    public:
        static auto NewRandomDouble() {
            return std::make_unique<RandomDouble>();
        }

        explicit RandomDouble(double min = 0.0, double max = 1.0)
                : uniform(min, max) { }

        // This function does not support concurrent entry.
        double nextValue() override {
            return uniform(*GetThreadLocalRandomGenerator());
        }

        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }

    private:
        std::uniform_real_distribution<double> uniform;
    };
}
