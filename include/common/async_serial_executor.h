//
// Created by user on 23-5-24.
//

#include <thread>
#include <functional>
#include <queue>

#include "common/cv_wrapper.h"

namespace util {
    class AsyncSerialExecutor {
    public:
        AsyncSerialExecutor() : thread(&AsyncSerialExecutor::run, this) { }

        ~AsyncSerialExecutor() {
            std::function<void()> task;
            cv.notify_one([&] {
                while (!tasks.empty()) {
                    tasks.pop();
                }    // drain queue
                tasks.emplace(nullptr);
            });
            thread.join();
        }

        bool addTask(std::function<void()> task) {
            cv.notify_one([&] {
                tasks.push(std::move(task));
            });
            return true;
        }

        void run() {
            pthread_setname_np(pthread_self(), "async_exec");
            std::function<void()> task = nullptr;
            while (true) {
                cv.wait([&] {
                    if (tasks.empty()) {
                        return false;
                    }
                    task = std::move(tasks.front());
                    tasks.pop();
                    return true;
                });
                if (task == nullptr) {
                    return;
                }
                task();
            }
        }

    private:
        util::CVWrapper cv;
        std::queue<std::function<void()>> tasks;
        std::thread thread;
    };
}
