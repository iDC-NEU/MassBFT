//
// Created by peng on 10/18/22.
//

#pragma once

#include <memory>
#include <random>
#include <memory>
#include "glog/logging.h"
#include "ycsb/core/generator/generator.h"

namespace ycsb::utils {
    class RandomUINT64 : public core::NumberGenerator {
    public:
        static auto NewRandomUINT64() {
            return std::make_unique<RandomUINT64>();
        }

        explicit RandomUINT64(uint64_t lb=0, uint64_t ub=std::numeric_limits<uint64_t>::max())
                : uniform(lb, ub) { }
        // This function does not support concurrent entry.
        uint64_t nextValue() override {
            return uniform(*GetThreadLocalRandomGenerator());
        }
        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }
    private:
        std::uniform_int_distribution<uint64_t> uniform;
    };
}
