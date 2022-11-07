//
// Created by peng on 10/18/22.
//

#pragma once

#include <memory>
#include <valarray>
#include "ycsb/core/generator/generator.h"
#include "ycsb/core/common/random_double.h"

namespace ycsb::core {
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
    public:
        constexpr static const auto EXPONENTIAL_PERCENTILE_PROPERTY = "exponential.percentile";
        constexpr static const double EXPONENTIAL_PERCENTILE_DEFAULT = 95;
        // What fraction of the dataset should be accessed exponential.percentile of the time?
        constexpr static const auto EXPONENTIAL_FRAC_PROPERTY = "exponential.frac";
        constexpr static const auto EXPONENTIAL_FRAC_DEFAULT = 0.8571428571;  // 1/7
    private:
        /**
         * The exponential constant to use.
         */
        double gamma;
        std::unique_ptr<DoubleGenerator> randomDouble;
    public:
        static auto NewExponentialGenerator(uint64_t seed, double percentile, double range) {
            return std::make_unique<ExponentialGenerator>(seed, percentile, range);
        }

        ExponentialGenerator(uint64_t seed, double percentile, double range) {
            gamma = -std::log(1.0 - percentile / 100.0) / range;  //1.0/mean;
            randomDouble = utils::RandomDouble::NewRandomDouble(seed);
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
