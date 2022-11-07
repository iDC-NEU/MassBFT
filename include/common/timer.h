//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_TIMER_H
#define NEUCHAIN_PLUS_TIMER_H

#include <chrono>
namespace util {
    class Timer {
    public:
        Timer() :startTime{Clock::now()} { }

        inline void start() { startTime = Clock::now(); }

        inline double end() {
            auto t = Clock::now();
            auto span = std::chrono::duration_cast<Duration>(t - startTime);
            return span.count();
        }

        static inline void sleep_ns(time_t t) {
            timespec req{};
            req.tv_sec = t / 1000000000;
            req.tv_nsec = t - req.tv_sec * 1000000000;
            nanosleep(&req, nullptr);
        }

        static inline void sleep_sec(double t) {
            if (t <= 0) {
                return;
            }
            timespec req{};
            req.tv_sec = static_cast<int>(t);
            req.tv_nsec = static_cast<int64_t>(1e9 * (t - (double)req.tv_sec));
            nanosleep(&req, nullptr);
        }

        static inline time_t time_now_ns() {
            timespec ts{};
            clock_gettime(CLOCK_REALTIME, &ts);
            return (ts.tv_sec*1000000000 + ts.tv_nsec);
        }

        static inline double span_sec(time_t previous, time_t now = time_now_ns()) {
            return static_cast<double>(now - previous) / 1e9;
        }

    private:
        typedef std::chrono::high_resolution_clock Clock;
        typedef std::chrono::duration<double> Duration;
        Clock::time_point startTime;
    };
}

#endif //NEUCHAIN_PLUS_TIMER_H
