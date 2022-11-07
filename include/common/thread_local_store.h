//
// Created by peng on 11/5/22.
//

#pragma once
namespace util {
    template<typename T>
    class ThreadLocalStore {
    public:
        static inline T* Get() {
            static thread_local T inst;
            return &inst;
        }

        ~ThreadLocalStore() = default;

    private:
        ThreadLocalStore() = default;
    };

}