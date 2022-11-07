//
// Created by peng on 10/17/22.
//

#pragma once

#include <valarray>
#include <mutex>
#include "glog/logging.h"
#include "ycsb/core/common/random_double.h"
namespace ycsb::core {
    /**
     * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than
     * others, according to a zipfian distribution. When you construct an instance of this class, you specify the number
     * of items in the set to draw from, either by specifying an itemcount (so that the sequence is of items from 0 to
     * itemcount-1) or by specifying a min and a max (so that the sequence is of items from min to max inclusive). After
     * you construct the instance, you can change the number of items by calling nextInt(itemcount) or nextLong(itemcount).
     *
     * Note that the popular items will be clustered together, e.g. item 0 is the most popular, item 1 the second most
     * popular, and so on (or min is the most popular, min+1 the next most popular, etc.) If you don't want this clustering,
     * and instead want the popular items scattered throughout the item space, then use ScrambledZipfianGenerator instead.
     *
     * Be aware: initializing this generator may take a long time if there are lots of items to choose from (e.g. over a
     * minute for 100 million objects). This is because certain mathematical values need to be computed to properly
     * generate a zipfian skew, and one of those values (zeta) is a sum sequence from 1 to n, where n is the itemcount.
     * Note that if you increase the number of items in the set, we can compute a new zeta incrementally, so it should be
     * fast unless you have added millions of items. However, if you decrease the number of items, we recompute zeta from
     * scratch, so this can take a long time.
     *
     * The algorithm used here is from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
     */
    class ZipfianGenerator : public NumberGenerator {
    public:
        // The default zipfian constant
        constexpr static const double ZIPFIAN_CONSTANT = 0.99;
        // Max item can generate.
        constexpr static const uint64_t MAX_NUMBER_ITEMS = (UINT64_MAX >> 24);
    private:
        // Number of items.
        const uint64_t items;
        // Min item to generate.
        const uint64_t base;
        // The zipfian constant to use.
        const double zipfianConstant;
        // Computed parameters for generating the distribution.
        double alpha, zetaN, eta;
        // theta = zipfianConstant
        // zeta(2, theta);
        const double theta, zeta2theta;
        // The number of items used to compute zetan the last time.
        uint64_t countForZeta;
        /**
         * Flag to prevent problems. If you increase the number of items the zipfian generator is allowed to choose from,
         * this code will incrementally compute a new zeta value for the larger itemcount. However, if you decrease the
         * number of items, the code computes zeta from scratch; this is expensive for large itemsets.
         * Usually this is not intentional; e.g. one thread thinks the number of items is 1001 and calls "nextLong()" with
         * that item count; then another thread who thinks the number of items is 1000 calls nextLong() with itemcount=1000
         * triggering the expensive recomputation. (It is expensive for 100 million items, not really for 1000 items.) Why
         * did the second thread think there were only 1000 items? maybe it read the item count before the first thread
         * incremented it. So this flag allows you to say if you really do want that recomputation. If true, then the code
         * will recompute zeta if the itemcount goes down. If false, the code will assume itemcount only goes up, and never
         * recompute.
         */
        const bool allowItemCountDecrease = false;
        // the random double[0,1] generator
        std::unique_ptr<DoubleGenerator> randomDouble;

        std::mutex mutex;
    public:
        /**
         * Create a zipfian generator for the specified number of items using the specified zipfian constant.
         *
         * @param items The number of items in the distribution.
         * @param zipfianConstant The zipfian constant to use.
         */
        explicit ZipfianGenerator(uint64_t num_items)
                : ZipfianGenerator(0, num_items - 1, ZIPFIAN_CONSTANT) { }
        /**
         * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant.
         * @param min The smallest integer to generate in the sequence.
         * @param max The largest integer to generate in the sequence.
         * @param zipfianConstant The zipfian constant to use.
         */
        ZipfianGenerator(uint64_t min, uint64_t max, double zipfianConstant = ZIPFIAN_CONSTANT)
                : ZipfianGenerator(min, max, zipfianConstant, ZetaStatic(0, max - min + 1, zipfianConstant, 0)) { }
        /**
         * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant, using
         * the precomputed value of zeta.
         *
         * @param min The smallest integer to generate in the sequence.
         * @param max The largest integer to generate in the sequence.
         * @param zipfianconstant The zipfian constant to use.
         * @param zetan The precomputed zeta constant.
         */
        ZipfianGenerator(uint64_t min, uint64_t max, double zipfianConstant, double zetaN)
                : items(max - min + 1),
                  base(min),
                  zipfianConstant(zipfianConstant),
                  alpha(0), zetaN(zetaN), eta(0),
                  theta(this->zipfianConstant), zeta2theta{zeta(0, 2, theta, 0)},
                  countForZeta(items) {
            LOG_ASSERT(items >= 2 && items < MAX_NUMBER_ITEMS);
            randomDouble = utils::RandomDouble::NewRandomDouble();
            alpha = 1.0 / (1.0 - theta);
            eta = calculateEta();

            ZipfianGenerator::nextValue();
        }

    private:
        [[nodiscard]] inline double calculateEta() const {
            return (1 - std::pow(2.0 / (double)items, 1 - theta)) / (1 - zeta2theta / zetaN);
        }
        /**
         * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
         * has n items now but used to have st items. Use the zipfian constant thetaVal. Remember the new value of
         * n so that if we change the itemcount, we'll know to recompute zeta.
         *
         * @param st=0 The number of items used to compute the last initialSum
         * @param n=items The number of items to compute zeta over.
         * @param thetaVal=theta The zipfian constant.
         * @param initialSum=0 The value of zeta we are computing incrementally from.
         */
        inline double zeta(uint64_t st, uint64_t n, double thetaVal, double initialSum) {
            countForZeta = n;
            return ZetaStatic(st, n, thetaVal, initialSum);
        }

    public:
        /**
         * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
         * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
         * n so that if we change the itemcount, we'll know to recompute zeta.
         * @param st The number of items used to compute the last initialSum
         * @param n The number of items to compute zeta over.
         * @param theta The zipfian constant.
         * @param initialSum The value of zeta we are computing incrementally from.
         */
        static double ZetaStatic(uint64_t st, uint64_t n, double theta, double initialSum) {
            double sum = initialSum;
            for (uint64_t i = st; i < n; i++) {
                sum += 1 / (std::pow(i + 1, theta));
            }
            return sum;
        }
    public:
        static auto NewZipfianGenerator(uint64_t min, uint64_t max) {
            auto generator = std::make_unique<ZipfianGenerator>(min, max, ZipfianGenerator::ZIPFIAN_CONSTANT);
            return generator;
        }

        /**
         * Generate the next item as a long.
         *
         * @param itemCount The number of items in the distribution.
         * @return The next item in the sequence.
         */
        uint64_t nextLong(uint64_t itemCount) {
            // from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et, al. SIGMOD 1994
            if (itemCount != countForZeta) {
                std::lock_guard lock(mutex);
                // have to recompute zetan and eta, since they depend on itemCount
                if (itemCount > countForZeta) {
                    DLOG(WARNING) << "WARNING: Incrementally recomputing Zipfian distribution. (itemCount=" << itemCount
                                  << "countForZeta=" << countForZeta << ")";
                    // we have added more items. can compute zetan incrementally, which is cheaper
                    zetaN = zeta(countForZeta, itemCount, theta, zetaN);
                    eta = calculateEta();
                } else if ((itemCount < countForZeta) && (allowItemCountDecrease)) {
                    // have to start over with zetan
                    // note : for large itemsets, this is very slow. so don't do it!
                    // TODO: can also have a negative incremental computation, e.g. if you decrease the number of items,
                    //  then just subtract the zeta sequence terms for the items that went away. This would be faster than
                    //  recomputing from scratch when the number of items decreases
                    DLOG(WARNING) << "WARNING: Recomputing Zipfian distribtion. This is slow and should be avoided. (itemCount="
                                  << itemCount << " countForZeta=" << countForZeta << ")";
                    zetaN = zeta(0, itemCount, theta, 0);
                    eta = calculateEta();
                }
            }
            double u = randomDouble->nextValue();
            double uz = u * zetaN;
            if (uz < 1.0) {
                return base;
            }
            if (uz < 1.0 + std::pow(0.5, theta)) {
                return base + 1;
            }
            uint64_t ret = base + (uint64_t)((double)itemCount * std::pow(eta * u - eta + 1, alpha));
            setLastValue(ret);
            return ret;
        }
        /**
         * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by
         * the 1st, followed by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the
         * next most popular, etc.) If you want the popular items scattered throughout the item space, use
         * ScrambledZipfianGenerator instead.
         */
        uint64_t nextValue() override {
            return nextLong(items);
        }

        double mean() override {
            CHECK(false) << "@todo implement ZipfianGenerator.mean()";
            return -1;
        }
    };
}
