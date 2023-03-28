//
// Created by peng on 2/11/23.
//

#ifndef NBP_RAFT_LOG_H
#define NBP_RAFT_LOG_H

#include "common/concurrent_queue.h"
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <butil/iobuf.h>

namespace util::raft {
    template<class T=butil::IOBuf>
    class RaftLog {
    public:
        template<typename Func>
        requires requires(Func f, const std::vector<T>* logs) { f(logs); }
        void read(Func f) const {
            std::shared_lock lock(mutex);
            f(static_cast<const std::vector<T>*>(&logs));
        }

        template<typename Func>
        requires requires(Func f, std::vector<T>* logs) { f(logs); }
        void write(Func f) {
            std::unique_lock lock(mutex);
            f(&logs);
        }

        const std::vector<T>& get_log_for_read() const {
            mutex.lock_shared();
            return logs;
        }

        void restore_read() const {
            mutex.unlock_shared();
        }

        std::vector<T>& get_log_for_write() {
            mutex.lock();
            return logs;
        }

        void restore_write() {
            mutex.unlock();
        }

        bool enqueue_request(T&& item) {
            return queue.enqueue(std::forward<T>(item));
        }

        void dequeue_request_blocked(T* item) {
            queue.wait_dequeue(*item);
        }

    private:
        mutable std::shared_mutex mutex;
        util::BlockingConcurrentQueue<T> queue;
        std::vector<T> logs;
    };
}

#endif //NBP_RAFT_LOG_H
