//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include "client/core/common/random_uint64.h"

namespace client::core {
    class NonUniformGenerator : public NumberGenerator {
    public:
        explicit NonUniformGenerator(uint64_t a, uint64_t lb, uint64_t ub, uint64_t C = 42)
                : generator1(0, a), generator2(lb, ub), lb(lb), ub(ub), C(C) { }

        uint64_t nextValue() override {
            return (((generator1.nextValue() | generator2.nextValue()) + C) % (ub - lb + 1)) + lb;
        }

        double mean() override {
            CHECK(false) << "Not implement!";
            return -1;
        }
    private:
        utils::RandomUINT64 generator1;
        utils::RandomUINT64 generator2;
        uint64_t lb, ub, C;
    };
}
