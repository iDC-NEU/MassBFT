//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include "client/core/common/random_uint64.h"

namespace client::core {
    class UniformLongExceptGenerator : public NumberGenerator {
    public:
        explicit UniformLongExceptGenerator(uint64_t lb, uint64_t ub, uint64_t v)
                : v(v), generator(lb, std::max(lb, ub)) { }

        uint64_t nextValue() override {
            auto r = generator.nextValue();
            if (r >= v) {
                return r + 1;
            } else {
                return r;
            }
        }

        double mean() override {
            CHECK(false) << "Not implement!";
            return -1;
        }

    private:
        uint64_t v;
        utils::RandomUINT64 generator;
    };
}
