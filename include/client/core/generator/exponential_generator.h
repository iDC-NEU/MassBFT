//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/common/random_double.h"
#include <memory>
#include <valarray>

namespace client::core {
    /**
     * A generator of an exponential distribution. It produces a sequence
     * of time intervals according to an exponential
     * distribution.  Smaller intervals are more frequent than larger
     * ones, and there is no bound on the length of an interval.  When you
     * construct an instance of this class, you specify a parameter gamma,
     * which corresponds to the rate at which events occur.
     * Alternatively, 1/gamma is the average length of an interval.
     */
    class ExponentialGenerator : public NumberGenerator {
    private:
        /**
         * The exponential constant to use.
         */
        double gamma;
        std::unique_ptr<DoubleGenerator> randomDouble;
    public:
        static auto NewExponentialGenerator(double percentile, double range) {
            return std::make_unique<ExponentialGenerator>(percentile, range);
        }

        ExponentialGenerator(double percentile, double range) {
            gamma = -std::log(1.0 - percentile / 100.0) / range;  //1.0/mean;
            randomDouble = utils::RandomDouble::NewRandomDouble();
        }

        /**
         * Generate the next item as a long. This distribution will be skewed toward lower values; e.g. 0 will
         * be the most popular, 1 the next most popular, etc.
         * @return The next item in the sequence.
         */
        uint64_t nextValue() override {
            return uint64_t(-std::log(randomDouble->nextValue()) / gamma);
        }

        double mean() override {
            return 1.0 / gamma;
        }
    };
}
