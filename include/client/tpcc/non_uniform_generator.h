//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include "client/core/common/random_uint64.h"

namespace client::core {
    class NonUniformGenerator : public NumberGenerator {
    public:
        // NURand(A, x, y) = (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x
        explicit NonUniformGenerator(uint64_t A, uint64_t x, uint64_t y, uint64_t C = 42)
                : generator1(0, A), generator2(x, y), x(x), y(y), C(C) {
            DCHECK((y - x + 1) > 0);
            // C is a run-time constant randomly chosen within [0 .. A] that can be varied without altering performance.
            DCHECK(C <= A);
        }

        uint64_t nextValue() override {
            return (((generator1.nextValue() | generator2.nextValue()) + C) % (y - x + 1)) + x;
        }

        double mean() override {
            CHECK(false) << "Not implement!";
            return -1;
        }
    private:
        utils::RandomUINT64 generator1;
        utils::RandomUINT64 generator2;
        uint64_t x, y, C;
    };
}
