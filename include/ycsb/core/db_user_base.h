//
// Created by user on 23-5-5.
//

#ifndef NBP_DB_USER_BASE_H
#define NBP_DB_USER_BASE_H

#include <map>
#include <queue>
#include <memory>
#include <thread>
#include <cassert>
#include <functional>
#include <cstdint>
#include "common/timer.h"
#include "common/light_weight_semaphore.hpp"

using tid_size_t = uint64_t;
using epoch_size_t = uint64_t;

namespace Utils {
    struct Request;
}

class ZMQClient;

namespace BlockBench {
    /*
     *  Provide basic ops for monitor tx generator workers.
     */
    class DBUserBase {
    public:
        DBUserBase();

        virtual ~DBUserBase();

        template<typename Callable, typename... Args>
        static void RateGenerator(uint32_t benchmarkTime, uint32_t txRate, Callable &&_f, Args &&... _args) {
            moodycamel::LightweightSemaphore sema;
            auto totalOp = benchmarkTime*txRate;
            auto waitTime = 1.0 / txRate;
            std::thread t([&] {
                for (size_t i = 0; i < totalOp; ++i) {
                    util::Timer::sleep_sec(waitTime);
                    sema.signal();
                }
            });
            for (size_t i = 0; i < totalOp; ++i) {
                _f(std::forward<Args>(_args)...);
                sema.wait();
            }
            t.join();
        }

        // worker call this function, signal, init by main() func
        std::function<void(const std::string &)> addPendingTransactionHandle;
        // monitor call this function, slot, to update the map param, init by main() func
        std::function<void(std::unordered_map<std::string, time_t> &)> updateTransactionMapHandle;

        // get latest committed block number.
        // return 0 when failed
        epoch_size_t getTipBlockNumber();

        using pollTxType = std::pair<std::string, bool>;

        // get a list of txs of specific block num from remote server.
        std::queue<pollTxType> pollTx(epoch_size_t block_number);

        static void StatusThread(std::unique_ptr<DBUserBase> db, std::atomic<bool> *finishSignal,
                                 double printInterval = 1.0, int startBlockHeight = 0);

    protected:
        std::function<void(const Utils::Request &)> sendInvokeRequest;

    private:
        std::vector<std::pair<std::string, std::unique_ptr<ZMQClient>>> invokeClient;
        std::vector<std::pair<std::string, std::unique_ptr<ZMQClient>>> queryClient;
        size_t trCount{};
    };
}

#endif //NBP_DB_USER_BASE_H
