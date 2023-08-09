//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/generator.h"
#include <memory>

namespace client::core {
    /**
     * A trivial integer generator that always returns the same value.
     *
     */
    class ConstantIntegerGenerator : public NumberGenerator {
    public:
        static auto NewConstantIntegerGenerator(uint64_t i) {
            return std::make_unique<ConstantIntegerGenerator> (i);
        }
        /**
         * @param i The integer that this generator will always return.
         */
        explicit ConstantIntegerGenerator(uint64_t i)
                : i(i) { }

        uint64_t nextValue() override {
            return i;
        }

        double mean() override {
            return (double)i;
        }

    private:
        const uint64_t i;
    };
}
