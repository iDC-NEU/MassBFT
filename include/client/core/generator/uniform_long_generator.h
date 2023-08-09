//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include "client/core/common/random_uint64.h"

namespace client::core {
    /**
     * Creates a generator that will return longs uniformly randomly from the
     * interval [lb,ub] inclusive (that is, lb and ub are possible values)
     * (lb and ub are possible values).
     *
     * @param lb the lower bound (inclusive) of generated values
     * @param ub the upper bound (inclusive) of generated values
     */
    class UniformLongGenerator : public NumberGenerator {
    public:
        static auto NewUniformLongGenerator(uint64_t lb, uint64_t ub) {
            return std::make_unique<UniformLongGenerator>(lb, ub);
        }

        explicit UniformLongGenerator(uint64_t lb, uint64_t ub)
                : generator(lb, ub) {
            m = double(lb + ub) / 2.0;
        }
        uint64_t nextValue() override {
            return generator.nextValue();
        }
        double mean() override {
            return m;
        }
    private:
        double m;
        utils::RandomUINT64 generator;
    };
}
