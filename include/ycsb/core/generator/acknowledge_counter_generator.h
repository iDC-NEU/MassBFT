//
// Created by peng on 10/18/22.
//

#pragma once

#include <mutex>
#include "ycsb/core/generator/counter_generator.h"
#include "ycsb/core/common/exception.h"

namespace ycsb::core {
    /**
     * A CounterGenerator that reports generated integers via lastInt()
     * only after they have been acknowledged.
     */
    class AcknowledgedCounterGenerator : public CounterGenerator {
        /** The size of the window of pending id ack's. 2^20 = {@value} */
        static const int WINDOW_SIZE = 1 << 20;
        /** The mask to use to turn an id into a slot in {@link #window}. */
        static const int WINDOW_MASK = WINDOW_SIZE - 1;
        std::vector<bool> window;
        volatile uint64_t  limit{};
        std::mutex mutex;

    public:
        static auto NewAcknowledgedCounterGenerator(uint64_t startWith) {
            return std::make_unique<AcknowledgedCounterGenerator> (startWith);
        }
        /**
         * Create a counter that starts at startWith.
         */
        explicit AcknowledgedCounterGenerator(uint64_t startWith)
                : CounterGenerator(startWith) {
            window.resize(WINDOW_SIZE);
            limit = startWith - 1;
        }
        /**
         * In this generator, the highest acknowledged counter value
         * (as opposed to the highest generated counter value).
         */
        uint64_t lastValue() override {
            return limit;
        }

        /**
         * Make a generated counter value available via lastInt().
         */

        void acknowledge(int value) {
            const int currentSlot = value & WINDOW_MASK;
            if (window[currentSlot]) {
                throw utils::WorkloadException("Too many unacknowledged insertion keys.");
            }
            window[currentSlot] = true;
            std::lock_guard lock(mutex);
            // TODO: use ReentrantLock instead
            // move a contiguous sequence from the window
            // over to the "limit" variable
            try {
                // Only loop through the entire window at most once.
                auto beforeFirstSlot = (limit & WINDOW_MASK);
                auto index = limit + 1;
                for (; index != beforeFirstSlot; ++index) {
                    int slot = (int)(index & WINDOW_MASK);
                    if (!window[slot]) {
                        break;
                    }

                    window[slot] = false;
                }

                limit = index - 1;
            } catch (const std::exception& e) {
                LOG(INFO) << "an exception occurred: " << e.what();
            }
        }
    };

}
