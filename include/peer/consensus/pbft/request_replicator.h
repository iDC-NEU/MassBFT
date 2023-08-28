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
        void batchingFunction() {
            pthread_setname_np(pthread_self(), "batch_leader");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            auto currentBatchSize = 0;
            auto unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);

            auto batchingFunc = [&]() -> bool {
                if (currentBatchSize == 0) {
                    return true;
                }
                unorderedRequests.resize(currentBatchSize);
                DLOG(INFO) << "Leader batch a block, size: " << currentBatchSize;
                if (_batchCallback && !_batchCallback(std::move(unorderedRequests))) {
                    LOG(WARNING) << "Batch call back return false!";
                    return false;
                }
                currentBatchSize = 0;   // reset
                unorderedRequests = std::vector<std::unique_ptr<proto::Envelop>>(_batchConfig.maxBatchSize);
                return true;
            };

            auto timer = util::Timer();

            auto callback = [&](zmq::message_t message, std::chrono::milliseconds* timeout) -> bool {
                if (_leaderStopSignal.load(std::memory_order_relaxed)) {
                    return false;
                }
                if (currentBatchSize == 0) {
                    timer.start();
                    if (!_sendToPeer->send(beginSeparator)) {
                        return false;   // send failure
                    }
                }
                auto timeLeftMs = _batchConfig.timeoutMs - static_cast<int>(timer.end_ns() / 1000 / 1000);
                *timeout = std::chrono::milliseconds();

                auto envelop = std::make_unique<proto::Envelop>();
                envelop->setSerializedMessage(message.to_string());
                if (!envelop->deserializeFromString()) {
                    LOG(WARNING) << "Deserialize user request failed.";
                    return true;
                }

                unorderedRequests[currentBatchSize] = std::move(envelop);
                currentBatchSize += 1;

                // send to other peers
                if (!_sendToPeer->send(std::move(message))) {
                    return false;   // send failure
                }

                if (timeLeftMs <= 0) {
                    return batchingFunc();  // timeout and batch is not empty
                }
                if (currentBatchSize >= _batchConfig.maxBatchSize) {
                    return batchingFunc();  // batch is full
                }
                return true;
            };

            _receiveFromUser->receive(callback);
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

                if (message.to_string_view() == beginSeparator) {
                    return batchingFunc();  // batch is full
                }

                auto envelop = std::make_unique<proto::Envelop>();
                envelop->setSerializedMessage(message.to_string());
                if (!envelop->deserializeFromString()) {
                    LOG(WARNING) << "Deserialize user request failed.";
                    return true;
                }

                unorderedRequests[currentBatchSize] = std::move(envelop);
                currentBatchSize += 1;
                return true;
            };

            _receiveFromPeer->receive(callback);
        }


    private:
        Config _batchConfig;
        std::atomic<bool> _leaderStopSignal;
        std::atomic<bool> _followerStopSignal;

        inline static const std::string beginSeparator = "begin_sep";

        // receive from user as a server
        std::shared_ptr<util::ZMQInstance> _receiveFromUser;
        std::unique_ptr<std::thread> _batchingThread;
        // listening to leader peer as a client
        std::shared_ptr<util::ZMQInstance> _receiveFromPeer;
        std::unique_ptr<std::thread> _followerThread;
        // send to follower as a server
        std::shared_ptr<util::ZMQInstance> _sendToPeer;

        std::function<bool(std::vector<std::unique_ptr<proto::Envelop>> unorderedRequests)> _batchCallback;
    };
}
