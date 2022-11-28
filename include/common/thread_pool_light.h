#pragma once

#include <atomic>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>

#include "concurrentqueue.h"
#include "lightweightsemaphore.h"

namespace util {

    template<class T>
    inline void wait_for_sema(T& sema, int cnt) {
        for (int i=cnt; i>0; i-=(int)sema.waitMany(i));
    }

    template<class T>
    inline void wait_for_sema(T& sema) {
        while(!sema.wait());
    }

    inline auto NewSema() {
        return moodycamel::LightweightSemaphore();
    }

    using concurrency_t = std::invoke_result_t<decltype(std::thread::hardware_concurrency)>;

    class [[nodiscard]] thread_pool_light {
    public:
        /**
         * @brief Construct a new thread pool.
         *
         * @param thread_count_ The number of threads to use. The default value is the total number of hardware threads available, as reported by the implementation. This is usually determined by the number of cores in the CPU. If a core is hyperthreaded, it will count as two threads.
         */
        explicit thread_pool_light(const concurrency_t thread_count_ = 0)
            : thread_count(determine_thread_count(thread_count_)), tasks(), threads(std::make_unique<std::thread[]>(determine_thread_count(thread_count_))) {
            create_threads();
        }

        /**
         * @brief Destruct the thread pool. Waits for all tasks to complete, then destroys all threads.
         */
        ~thread_pool_light() {
            destroy_threads();
        }

        /**
         * @brief Get the number of threads in the pool.
         *
         * @return The number of threads.
         */
        [[nodiscard]] concurrency_t get_thread_count() const {
            return thread_count;
        }

        /**
         * @brief Parallelize a loop by automatically splitting it into blocks and submitting each block separately to the queue. The user must use wait_for_tasks() or some other method to ensure that the loop finishes executing, otherwise bad things will happen.
         *
         * @tparam F The type of the function to loop through.
         * @tparam T1 The type of the first index in the loop. Should be a signed or unsigned integer.
         * @tparam T2 The type of the index after the last index in the loop. Should be a signed or unsigned integer. If T1 is not the same as T2, a common type will be automatically inferred.
         * @tparam T The common type of T1 and T2.
         * @param first_index The first index in the loop.
         * @param index_after_last The index after the last index in the loop. The loop will iterate from first_index to (index_after_last - 1) inclusive. In other words, it will be equivalent to "for (T i = first_index; i < index_after_last; ++i)". Note that if index_after_last == first_index, no blocks will be submitted.
         * @param loop The function to loop through. Will be called once per block. Should take exactly two arguments: the first index in the block and the index after the last index in the block. loop(start, end) should typically involve a loop of the form "for (T i = start; i < end; ++i)".
         * @param num_blocks The maximum number of blocks to split the loop into. The default is to use the number of threads in the pool.
         */
        template <typename F, typename T1, typename T2, typename T = std::common_type_t<T1, T2>>
        void push_loop(T1 first_index_, T2 index_after_last_, F&& loop, size_t num_blocks = 0) {
            T first_index = static_cast<T>(first_index_);
            T index_after_last = static_cast<T>(index_after_last_);
            if (num_blocks == 0)
                num_blocks = thread_count;
            if (index_after_last < first_index)
                std::swap(index_after_last, first_index);
            auto total_size = static_cast<size_t>(index_after_last - first_index);
            auto block_size = static_cast<size_t>(total_size / num_blocks);
            if (block_size == 0) {
                block_size = 1;
                num_blocks = (total_size > 1) ? total_size : 1;
            }
            if (total_size > 0) {
                for (size_t i = 0; i < num_blocks; ++i)
                    push_task(std::forward<F>(loop), static_cast<T>(i * block_size) + first_index, (i == num_blocks - 1) ? index_after_last : (static_cast<T>((i + 1) * block_size) + first_index));
            }
        }

        /**
         * @brief Parallelize a loop by automatically splitting it into blocks and submitting each block separately to the queue. The user must use wait_for_tasks() or some other method to ensure that the loop finishes executing, otherwise bad things will happen. This overload is used for the special case where the first index is 0.
         *
         * @tparam F The type of the function to loop through.
         * @tparam T The type of the loop indices. Should be a signed or unsigned integer.
         * @param index_after_last The index after the last index in the loop. The loop will iterate from 0 to (index_after_last - 1) inclusive. In other words, it will be equivalent to "for (T i = 0; i < index_after_last; ++i)". Note that if index_after_last == 0, no blocks will be submitted.
         * @param loop The function to loop through. Will be called once per block. Should take exactly two arguments: the first index in the block and the index after the last index in the block. loop(start, end) should typically involve a loop of the form "for (T i = start; i < end; ++i)".
         * @param num_blocks The maximum number of blocks to split the loop into. The default is to use the number of threads in the pool.
         */
        template <typename F, typename T>
        void push_loop(const T index_after_last, F&& loop, const size_t num_blocks = 0) {
            push_loop(0, index_after_last, std::forward<F>(loop), num_blocks);
        }

        /**
         * @brief Push a function with zero or more arguments, but no return value, into the task queue. Does not return a future, so the user must use wait_for_tasks() or some other method to ensure that the task finishes executing, otherwise bad things will happen.
         *
         * @tparam F The type of the function.
         * @tparam A The types of the arguments.
         * @param task The function to push.
         * @param args The zero or more arguments to pass to the function. Note that if the task is a class member function, the first argument must be a pointer to the object, i.e. &object (or this), followed by the actual arguments.
         */
        template <typename F, typename... A>
        void push_task(F&& task, A&&... args) {
            std::function<void()> task_function = std::bind(std::forward<F>(task), std::forward<A>(args)...);
            tasks.enqueue(task_function);
            sema.signal();
        }

        /**
         * @brief Submit a function with zero or more arguments into the task queue. If the function has a return value, get a future for the eventual returned value. If the function has no return value, get an std::future<void> which can be used to wait until the task finishes.
         *
         * @tparam F The type of the function.
         * @tparam A The types of the zero or more arguments to pass to the function.
         * @tparam R The return type of the function (can be void).
         * @param task The function to submit.
         * @param args The zero or more arguments to pass to the function. Note that if the task is a class member function, the first argument must be a pointer to the object, i.e. &object (or this), followed by the actual arguments.
         * @return A future to be used later to wait for the function to finish executing and/or obtain its returned value if it has one.
         */
        template <typename F, typename... A, typename R = std::invoke_result_t<std::decay_t<F>, std::decay_t<A>...>>
        [[nodiscard]] std::future<R> submit(F&& task, A&&... args) {
            std::function<R()> task_function = std::bind(std::forward<F>(task), std::forward<A>(args)...);
            std::shared_ptr<std::promise<R>> task_promise = std::make_shared<std::promise<R>>();
            push_task(
                    [task_function, task_promise] {
                        try {
                            if constexpr (std::is_void_v<R>) {
                                std::invoke(task_function);
                                task_promise->set_value();
                            } else {
                                task_promise->set_value(std::invoke(task_function));
                            }
                        }
                        catch (...) {
                            task_promise->set_exception(std::current_exception());
                        }
                    });
            return task_promise->get_future();
        }

    private:
        void create_threads() {
            running.store(true, std::memory_order_release);
            for (concurrency_t i = 0; i < thread_count; ++i) {
                threads[i] = std::thread(&thread_pool_light::worker, this);
            }
        }

        void destroy_threads() {
            running.store(false, std::memory_order_release);
            sema.signal(thread_count);
            for (concurrency_t i = 0; i < thread_count; ++i) {
                threads[i].join();
            }
        }

        [[nodiscard]] static concurrency_t determine_thread_count(const concurrency_t thread_count_) {
            if (thread_count_ > 0)
                return thread_count_;
            else {
                if (std::thread::hardware_concurrency() > 0)
                    return std::thread::hardware_concurrency();
                else
                    return 1;
            }
        }

        void worker() {
            while (running.load(std::memory_order_relaxed)) {
                std::function<void()> task;
                while(!sema.wait());    // wait a task
                while(running.load(std::memory_order_relaxed) && !tasks.try_dequeue(task));
                if (!running.load(std::memory_order_acquire)) {
                    return;
                }
                task();
            }
        }

        const concurrency_t thread_count;

        std::atomic<bool> running = false;

        moodycamel::LightweightSemaphore sema;

        moodycamel::ConcurrentQueue<std::function<void()>> tasks;

        std::unique_ptr<std::thread[]> threads = nullptr;
    };

}