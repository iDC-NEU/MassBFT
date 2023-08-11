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
                : _v(v), generator(lb, std::max(lb, ub-1)) {
            // special case when there is only one value
            if (lb == ub) {
                _v = ub + 1;
            }
        }

        uint64_t nextValue() override {
            auto r = generator.nextValue();
            // return value must exclude _v, so increase r
            // return [lb, v) U (v, ub] when lb < rb
            // or [lb, rb] when lb == rb
            if (r >= _v) {
                return r + 1;
            }
            return r;
        }

        double mean() override {
            CHECK(false) << "Not implement!";
            return -1;
        }

    private:
        uint64_t _v;
        utils::RandomUINT64 generator;
    };
}
