//
// Created by peng on 23-3-21.
//

#pragma once

#include "common/zeromq.h"
#include "common/timer.h"
#include "common/thread_pool_light.h"
#include "common/concurrent_queue.h"
#include "proto/user_request.h"

#include "bthread/butex.h"
#include "bthread/countdown_event.h"

namespace peer::consensus {
    // RequestCollector is used to collect requests from local users.
    // When the request is greater than the threshold or times out,
    // the callback function is called on the request collection.
    class RequestCollector {
    public:
        struct Config {
            int timeoutMs;
            int maxBatchSize;
        };

        explicit RequestCollector(const Config& config, int port)
                : _batchConfig(config), _tearDownSignal(false) {
            _subscriber = util::ZMQInstance::NewServer<zmq::socket_type::sub>(port);
        }

        RequestCollector(const RequestCollector&) = delete;

        RequestCollector(RequestCollector&&) = delete;

        ~RequestCollector() {
            _subscriber->shutdown();
            _tearDownSignal = true;
            if (_collectorThread) {
                _collectorThread->join();
            }
            if (_batchingThread) {
                _batchingThread->join();
            }
        }

        // If you want to process the batched envelop
        // The callback function may change at runtime
        void setBatchCallback(auto callback) {
            std::unique_lock guard(_batchCallbackMutex);
            _batchCallback = std::move(callback);
        }
        auto getBatchCallback() const {
            std::unique_lock guard(_batchCallbackMutex);
            return _batchCallback;
        }

        // If you want to validate an envelope in place
        void setValidateCallback(auto callback, std::shared_ptr<util::thread_pool_light> threadPool) {
            _validateCallback = std::move(callback);
            _threadPoolForBCCSP = std::move(threadPool);
        }

        void start() {
            if (_collectorThread || _batchingThread) {
                return; // already started
            }
            if (_validateCallback == nullptr || _validateCallback == nullptr) {
                _collectorThread = std::make_unique<std::thread>(&RequestCollector::collectorFunction<false>, this);
            } else {
                _collectorThread = std::make_unique<std::thread>(&RequestCollector::collectorFunction<true>, this);
            }
            _batchingThread = std::make_unique<std::thread>(&RequestCollector::batchingFunction, this);
        }

    protected:
        template<bool eagerValidate>
        void collectorFunction() {
            pthread_setname_np(pthread_self(), "usr_req_coll");
            while(!_tearDownSignal.load(std::memory_order_relaxed)) {
                auto ret = _subscriber->receive();
                if (ret == std::nullopt) {
                    return;  // socket dead
                }
                auto envelop = std::make_unique<proto::Envelop>();
                envelop->setSerializedMessage(ret->to_string());
                if (!envelop->deserializeFromString()) {
                    LOG(WARNING) << "Deserialize user request failed.";
                    continue;
                }
                if (eagerValidate) {
                    _threadPoolForBCCSP->push_task([e=envelop.release(), this] {
                        if (!_validateCallback(*e)) {
                            return; // validate error
                        }
                        if (!_requestsQueue.enqueue(std::unique_ptr<proto::Envelop>(e))) {
                            CHECK(false) << "Queue max size achieve!";
                        }
                    });
                } else {
                    if (!_requestsQueue.enqueue(std::move(envelop))) {
                        CHECK(false) << "Queue max size achieve!";
                    }
                }
            }
        }

        void batchingFunction() {
            pthread_setname_np(pthread_self(), "usr_req_proc");
            while(!_tearDownSignal.load(std::memory_order_relaxed)) {
                auto unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);
                auto timer = util::Timer();
                auto timeLeftUs = _batchConfig.timeoutMs * 1000;
                auto currentBatchSize = 0;
                while (true) {
                    if (_tearDownSignal.load(std::memory_order_relaxed)) {
                        return;
                    }
                    auto ret = _requestsQueue.wait_dequeue_bulk_timed(unorderedRequests.begin() + currentBatchSize,
                                                                      _batchConfig.maxBatchSize - currentBatchSize,
                                                                      timeLeftUs);
                    currentBatchSize += (int)ret;
                    if (currentBatchSize == 0) {   // We can not pass empty batch to replicator
                        timer.start();
                        continue;  // reset timer and retry
                    }
                    if (currentBatchSize == _batchConfig.maxBatchSize) {
                        break;  // batch is full
                    }
                    timeLeftUs = _batchConfig.timeoutMs * 1000 - static_cast<int>(timer.end_ns() / 1000);
                    if (timeLeftUs <= 0) {
                        break;  // timeout and batch is not empty
                    }
                }
                unorderedRequests.resize(currentBatchSize);
                DLOG(INFO) << "Batch a block, size: " << currentBatchSize;
                auto callback = getBatchCallback();
                if (callback == nullptr) {
                    LOG(WARNING) << "Batch call back is not set yet!";
                    continue;
                }
                if (!callback(std::move(unorderedRequests))) {
                    LOG(WARNING) << "Batch call back return false!";
                    continue;
                }
            }
        }

    private:
        const Config _batchConfig;
        std::atomic<bool> _tearDownSignal;
        std::unique_ptr<std::thread> _collectorThread;
        std::unique_ptr<std::thread> _batchingThread;
        // the size of the queue is 5000 (5000 cached requests)
        // TODO: consider limit the size of the queue
        util::BlockingConcurrentQueue<std::unique_ptr<proto::Envelop>> _requestsQueue;
        // Receiving requests from local clients (as a server)
        std::shared_ptr<util::ZMQInstance> _subscriber;
        // Call this function when forming a request batch
        mutable std::mutex _batchCallbackMutex;
        std::function<bool(std::vector<std::unique_ptr<proto::Envelop>> unorderedRequests)> _batchCallback;
        std::function<bool(const proto::Envelop& envelop)> _validateCallback;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
    };
}
