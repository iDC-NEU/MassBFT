//
// Created by peng on 10/18/22.
//

#pragma once

#include "client/core/generator/zipfian_generator.h"
#include "client/core/common/fnv_hash.h"
#include "glog/logging.h"
#include <memory>
#include <valarray>
#include <mutex>

namespace client::core {
    /**
     * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than
     * others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
     * of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
     * itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
     * you construct the instance, you can change the number of items by calling nextInt(itemcount) or nextLong(itemcount).
     * <p>
     * Unlike @ZipfianGenerator, this class scatters the "popular" items across the itemspace. Use this, instead of
     * @ZipfianGenerator, if you don't want the head of the distribution (the popular items) clustered together.
     */
    class ScrambledZipfianGenerator : public NumberGenerator {
        constexpr static const double ZETAN = 26.46902820178302;
        constexpr static const double USED_ZIPFIAN_CONSTANT = 0.99;
        constexpr static const uint64_t ITEM_COUNT = 10000000000L;
        std::unique_ptr<ZipfianGenerator> zipfianGenerator;
        uint64_t min, max, itemCount;
    public:
        static auto NewScrambledZipfianGenerator(uint64_t min, uint64_t max) {
            auto generator = std::make_unique<ScrambledZipfianGenerator>(min, max, ZipfianGenerator::ZIPFIAN_CONSTANT);
            return generator;
        }
        /**
         * Create a zipfian generator for the specified number of items.
         *
         * @param items The number of items in the distribution.
         */
        explicit ScrambledZipfianGenerator(uint64_t items)
        : ScrambledZipfianGenerator(0, items-1) {}
        /**
         * Create a zipfian generator for items between min and max.
         *
         * @param min The smallest integer to generate in the sequence.
         * @param max The largest integer to generate in the sequence.
         */
        ScrambledZipfianGenerator(uint64_t min, uint64_t max)
        : ScrambledZipfianGenerator(min, max, ZipfianGenerator::ZIPFIAN_CONSTANT) {}
        /**
         * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant. If you
         * use a zipfian constant other than 0.99, this will take a long time to complete because we need to recompute zeta.
         *
         * @param min             The smallest integer to generate in the sequence.
         * @param max             The largest integer to generate in the sequence.
         * @param zipfianconstant The zipfian constant to use.
         */
        ScrambledZipfianGenerator(uint64_t min, uint64_t max, double zipfianConstant)
        : min(min), max(max), itemCount(max-min+1) {
            if (itemCount != ITEM_COUNT) {
                LOG(WARNING) << "ScrambledZipfianGenerator init with item count: " << itemCount << " != " << ITEM_COUNT;
            }
            if (zipfianConstant == USED_ZIPFIAN_CONSTANT) {
                zipfianGenerator = std::make_unique<ZipfianGenerator>(0, ITEM_COUNT, zipfianConstant, ZETAN);
            } else {
                zipfianGenerator = std::make_unique<ZipfianGenerator>(0, ITEM_COUNT, zipfianConstant);
            }
        }
    public:
        /**
         * Return the next long in the sequence.
         */
        uint64_t nextValue() override {
            auto ret = zipfianGenerator->nextValue();
            ret = min + client::utils::FNVHash64(ret) % itemCount;
            setLastValue(ret);
            return ret;
        }
        /**
         * since the values are scrambled (hopefully uniformly), the mean is simply the middle of the range.
         */
        double mean() override {
            return double((min) + max) / 2.0;
        }
    };
}
