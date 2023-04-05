//
// Created by peng on 11/6/22.
//

#pragma once

#include <chrono>
#include <thread>

namespace util {
    class Timer {
    public:
        Timer() { start(); }

        inline void start() { startTime = std::chrono::system_clock::now(); }

        inline double end() {
            auto now = std::chrono::system_clock::now();
            auto span = std::chrono::duration_cast<Duration>(now - startTime);
            return span.count();
        }
        inline int64_t end_ns() {
            auto now = std::chrono::system_clock::now();
            auto span = std::chrono::duration_cast<std::chrono::nanoseconds>(now - startTime).count();
            return span;
        }

        static inline void sleep_ns(int64_t t) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(t));
        }

        static inline void sleep_ms(int64_t t) {
            std::this_thread::sleep_for(std::chrono::milliseconds(t));
        }

        static inline void sleep_sec(double t) {
            if (t <= 0) { return; }
            uint64_t span = static_cast<int64_t>(t * 1000 * 1000 * 1000);
            std::this_thread::sleep_for(std::chrono::nanoseconds(span));
        }

        static inline int64_t time_now_ns() {
            auto now = std::chrono::system_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
            return ns;
        }

        static inline int64_t time_now_ms() {
            auto now = std::chrono::system_clock::now();
            auto ns = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
            return ns;
        }

    private:
        typedef std::chrono::high_resolution_clock Clock;
        typedef std::chrono::duration<double> Duration;
        Clock::time_point startTime;
    };
}

