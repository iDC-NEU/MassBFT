//
// Created by peng on 23-3-21.
//

#pragma once

#include "common/zeromq.h"
#include "common/timer.h"
#include "proto/user_request.h"

namespace peer::consensus::v2 {
    // RequestReplicator is used to collect requests from local users.
    // When the request is greater than the threshold or times out,
    // the callback function is called on the request collection.
    class RequestReplicator {
    public:
        struct Config {
            int timeoutMs;
            int maxBatchSize;
        };

        explicit RequestReplicator(const Config& config)
                : _batchConfig(config) {
        }

        RequestReplicator(const RequestReplicator&) = delete;

        RequestReplicator(RequestReplicator&&) = delete;

        ~RequestReplicator() {
            _receiveFromUser->shutdown();
            _leaderStopSignal = true;
            _followerStopSignal = true;
            if (_receiveFromUserThread) {
                _receiveFromUserThread->join();
            }
            if (_batchingThread) {
                _batchingThread->join();
            }
            if (_followerThread) {
                _followerThread->join();
            }
        }

        void setBatchCallback(auto callback) {
            _batchCallback = std::move(callback);
        }

        void startLeader(int userPort, int leaderPort) {
            _leaderStopSignal = true;
            if (_batchingThread) {
                _batchingThread->join();
            }
            _leaderStopSignal = false;
            _receiveFromUser = util::ZMQInstance::NewServer<zmq::socket_type::sub>(userPort);
            _sendToPeer = util::ZMQInstance::NewServer<zmq::socket_type::pub>(leaderPort);
            {   // clear the queue
                std::unique_ptr<proto::Envelop> trash;
                while (_receiveFromUserQueue.try_dequeue(trash));
            }
            _receiveFromUserThread = std::make_unique<std::thread>(&RequestReplicator::collectorFunction, this);
            _batchingThread = std::make_unique<std::thread>(&RequestReplicator::batchingFunction, this);
        }

        void startFollower(const std::string& leaderIp, int leaderPort) {
            _followerStopSignal = true;
            if (_followerThread) {
                _followerThread->join();
            }
            _followerStopSignal = false;
            _receiveFromPeer = util::ZMQInstance::NewClient<zmq::socket_type::sub>(leaderIp, leaderPort);
            _followerThread = std::make_unique<std::thread>(&RequestReplicator::followerFunction, this);
        }

    protected:
        void collectorFunction() {
            pthread_setname_np(pthread_self(), "req_collector");
            while(true) {
                if (_leaderStopSignal.load(std::memory_order_relaxed)) {
                    return;
                }
                auto ret = _receiveFromUser->receive();
                if (ret == std::nullopt) {
                    return;  // socket dead
                }
                auto envelop = std::make_unique<proto::Envelop>();
                if (envelop->deserializeFromString(ret->to_string_view()) < 0) {
                    LOG(WARNING) << "Deserialize user request failed.";
                }
                _receiveFromUserQueue.enqueue(std::move(envelop));
            }
        }

        void batchingFunction() {
            pthread_setname_np(pthread_self(), "batch_leader");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            while(true) {
                auto unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);
                auto timer = util::Timer();
                auto timeLeftUs = _batchConfig.timeoutMs * 1000;
                auto currentBatchSize = 0;
                while (true) {
                    if (_leaderStopSignal.load(std::memory_order_relaxed)) {
                        return;
                    }
                    // wake up every 1ms
                    auto ret = _receiveFromUserQueue.wait_dequeue_bulk_timed(unorderedRequests.begin() + currentBatchSize,
                                                                             _batchConfig.maxBatchSize - currentBatchSize,
                                                                             std::min(timeLeftUs, 1000));
                    {   // batch send function
                        std::string buffer;
                        buffer.reserve((int)ret * 512);
                        for (int i = 0; i < (int)ret; i++) {
                            unorderedRequests[currentBatchSize + i]->serializeToString(&buffer, (int)buffer.size());
                        }
                        if (!_sendToPeer->send(buffer)) {
                            return;
                        }
                    }
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
                DLOG(INFO) << "Leader batch a block, size: " << currentBatchSize;
                // notice peer this block is end
                if (!_sendToPeer->send(endSeparator)) {
                    return;   // send failure
                }
                if (_batchCallback && !_batchCallback(std::move(unorderedRequests))) {
                    LOG(WARNING) << "Batch call back return false!";
                    continue;
                }
            }
        }

        void followerFunction() {
            pthread_setname_np(pthread_self(), "batch_follower");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            auto currentBatchSize = 0;
            auto unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);

            auto batchingFunc = [&]() -> bool {
                if (currentBatchSize == 0) {
                    return true;
                }
                unorderedRequests.resize(currentBatchSize);
                DLOG(INFO) << "Follower batch a block, size: " << currentBatchSize;
                if (_batchCallback && !_batchCallback(std::move(unorderedRequests))) {
                    LOG(WARNING) << "Batch call back return false!";
                    return false;
                }
                currentBatchSize = 0;   // reset
                unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);
                return true;
            };

            auto callback = [&](zmq::message_t message, std::chrono::milliseconds* timeout) -> bool {
                *timeout = std::chrono::milliseconds(10);
                if (_followerStopSignal.load(std::memory_order_relaxed)) {
                    return false;
                }

                if (message.to_string_view() == endSeparator) {
                    return batchingFunc();  // batch is full
                }

                int pos = 0;
                while (pos < (int)message.size()) {
                    if (currentBatchSize == (int)unorderedRequests.size()) {
                        LOG(WARNING) << "Max size exceed.";
                        return true;
                    }
                    auto envelop = std::make_unique<proto::Envelop>();
                    pos = envelop->deserializeFromString(message.to_string_view(), pos);
                    if (pos < 0) {
                        LOG(WARNING) << "Deserialize user request failed.";
                        return true;
                    }
                    unorderedRequests[currentBatchSize] = std::move(envelop);
                    currentBatchSize += 1;
                }
                return true;
            };

            _receiveFromPeer->receive(callback);
        }


    private:
        Config _batchConfig;
        std::atomic<bool> _leaderStopSignal;
        std::atomic<bool> _followerStopSignal;

        inline static const std::string endSeparator = "end_sep";

        // receive from user as a server
        std::unique_ptr<std::thread> _receiveFromUserThread;
        std::shared_ptr<util::ZMQInstance> _receiveFromUser;
        util::BlockingConcurrentQueue<std::unique_ptr<proto::Envelop>> _receiveFromUserQueue{};
        std::unique_ptr<std::thread> _batchingThread;
        // listening to leader peer as a client
        std::shared_ptr<util::ZMQInstance> _receiveFromPeer;
        std::unique_ptr<std::thread> _followerThread;
        // send to follower as a server
        std::shared_ptr<util::ZMQInstance> _sendToPeer;

        std::function<bool(std::vector<std::unique_ptr<proto::Envelop>> unorderedRequests)> _batchCallback;
    };
}
