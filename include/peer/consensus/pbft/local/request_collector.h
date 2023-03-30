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
            std::lock_guard guard(_batchCallbackMutex);
            _batchCallback = std::move(callback);
        }

        // If you want to validate an envelop in place
        void setValidateCallback(auto callback, std::shared_ptr<util::thread_pool_light> threadPool) {
            _validateCallback = std::move(callback);
            _threadPoolForBCCSP = std::move(threadPool);
        }

        void start() {
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
                        _requestsQueue.enqueue(std::unique_ptr<proto::Envelop>(e));
                    });
                } else {
                    _requestsQueue.enqueue(std::move(envelop));
                }
            }
        }

        void batchingFunction() {
            pthread_setname_np(pthread_self(), "usr_req_proc");
            while(!_tearDownSignal.load(std::memory_order_relaxed)) {
                auto unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);
                auto ret = _requestsQueue.wait_dequeue_bulk_timed(unorderedRequests.begin(), _batchConfig.maxBatchSize, _batchConfig.timeoutMs*1000);
                if (ret == 0) {
                    continue;   // We can not pass empty batch to replicator
                }
                unorderedRequests.resize(ret);
                std::lock_guard guard(_batchCallbackMutex);
                if (_batchCallback != nullptr) {
                    if (!_batchCallback(std::move(unorderedRequests))) {
                        LOG(WARNING) << "Batch call back return false!";
                        continue;
                    }
                }
            }
        }

    private:
        const Config _batchConfig;
        std::atomic<bool> _tearDownSignal;
        std::unique_ptr<std::thread> _collectorThread;
        std::unique_ptr<std::thread> _batchingThread;
        // the size of the queue is 5000 (5000 cached requests)
        util::BlockingConcurrentQueue<std::unique_ptr<proto::Envelop>, 5000> _requestsQueue;
        // Receiving requests from local clients (as a server)
        std::shared_ptr<util::ZMQInstance> _subscriber;
        // Call this function when forming a request batch
        std::mutex _batchCallbackMutex;
        std::function<bool(std::vector<std::unique_ptr<proto::Envelop>> unorderedRequests)> _batchCallback;
        std::function<bool(const proto::Envelop& envelop)> _validateCallback;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
    };
}