//
// Created by user on 23-3-28.
//

#pragma once

#include "blockingconcurrentqueue.h"

namespace util {
    template<size_t maxSize>
    struct MyDefaultTraits : public moodycamel::ConcurrentQueueDefaultTraits {
        static const int MAX_SEMA_SPINS = 100;
        static const size_t MAX_SUBQUEUE_SIZE = maxSize;
    };
    template<class T, size_t maxSize=moodycamel::details::const_numeric_max<size_t>::value>
    using BlockingConcurrentQueue = moodycamel::BlockingConcurrentQueue<T, MyDefaultTraits<maxSize>>;

    template<size_t maxSize>
    struct FewElementTraits : public moodycamel::ConcurrentQueueDefaultTraits {
        static const int MAX_SEMA_SPINS = 0;
        static const size_t BLOCK_SIZE = 4;
        static const size_t MAX_SUBQUEUE_SIZE = maxSize;
    };
    template<class T, size_t maxSize=32>
    using BlockingConcurrentQueueFewElements = moodycamel::BlockingConcurrentQueue<T, FewElementTraits<maxSize>>;
}