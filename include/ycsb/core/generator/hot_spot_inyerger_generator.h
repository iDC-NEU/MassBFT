//
// Created by peng on 10/18/22.
//

#pragma once

#include "ycsb/core/generator/counter_generator.h"
#include "ycsb/core/common/random_uint64.h"
#include "ycsb/core/common/random_double.h"

namespace ycsb::core {

    class HotspotIntegerGenerator : public NumberGenerator {
        uint64_t lowerBound, upperBound, hotInterval, coldInterval;
        double hotsetFraction, hotOpnFraction;
    public:
        static auto NewHotspotIntegerGenerator(uint64_t lowerBound, uint64_t upperBound, double hotsetFraction, double hotOpnFraction) {
            return std::make_unique<HotspotIntegerGenerator> (lowerBound, upperBound, hotsetFraction, hotOpnFraction);
        }

        /**
         * Create a generator for Hotspot distributions.
         *
         * @param lowerBound lower bound of the distribution.
         * @param upperBound upper bound of the distribution.
         * @param hotsetFraction percentage of data item
         * @param hotOpnFraction percentage of operations accessing the hot set.
         */
        HotspotIntegerGenerator(uint64_t lowerBound, uint64_t upperBound, double hotsetFraction, double hotOpnFraction) {
            if (hotsetFraction < 0.0 || hotsetFraction > 1.0) {
                LOG(ERROR) << "Hotset fraction out of range. Setting to 0.0";
                hotsetFraction = 0.0;
            }
            if (hotOpnFraction < 0.0 || hotOpnFraction > 1.0) {
                LOG(ERROR) << "Hot operation fraction out of range. Setting to 0.0";
                hotOpnFraction = 0.0;
            }
            if (lowerBound > upperBound) {
                LOG(ERROR) << "Upper bound of Hotspot generator smaller than the lower bound.  Swapping the values.";
                auto temp = lowerBound;
                lowerBound = upperBound;
                upperBound = temp;
            }
            this->lowerBound = lowerBound;
            this->upperBound = upperBound;
            this->hotsetFraction = hotsetFraction;
            auto interval = upperBound - lowerBound + 1;
            this->hotInterval = (uint64_t) ((double)interval * hotsetFraction);
            this->coldInterval = interval - hotInterval;
            this->hotOpnFraction = hotOpnFraction;
        }
        uint64_t nextValue() override {
            uint64_t value = 0;
            if (doubleGenerator.nextValue() < hotOpnFraction) {
                // Choose a value from the hot set.
                value = lowerBound + uintGenerator.nextValue() % hotInterval;
            } else {
                // Choose a value from the cold set.
                value = lowerBound + hotInterval + uintGenerator.nextValue() % coldInterval;
            }
            setLastValue(value);
            return value;
        }
        double mean() override {
            return hotOpnFraction * ((double)lowerBound + (double)hotInterval / 2.0)
                   + (1 - hotOpnFraction) * ((double)lowerBound + (double)hotInterval + (double)coldInterval / 2.0);
        }
    private:
        utils::RandomUINT64 uintGenerator;
        utils::RandomDouble doubleGenerator;
    };

}
