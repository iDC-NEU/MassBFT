//
// Created by user on 23-5-7.
//

#pragma once

#include "common/zeromq.h"
#include "common/property.h"
#include <thread>

#include "glog/logging.h"

namespace peer::consensus::v2 {
    // ActiveZMQReceiver acts as zmq clients, receive from 1 server
    class ActiveZMQReceiver {
    protected:
        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "lo_dec_rec");
            auto* instance=static_cast<ActiveZMQReceiver*>(ptr);
            while(true) {
                auto ret = instance->_sub->receive();
                if (ret == std::nullopt) {
                    break;  // socket dead
                }
                instance->_cb(ret->to_string());
            }
            return nullptr;
        }

    public:
        virtual ~ActiveZMQReceiver() {
            if (_sub) { _sub->shutdown(); }
            // join the event loop
            if (_thread) { _thread->join(); }
        }

        explicit ActiveZMQReceiver(auto&& cb) :_cb(std::forward<decltype(cb)>(cb)) { }

        ActiveZMQReceiver(ActiveZMQReceiver&&) = delete;

        ActiveZMQReceiver(const ActiveZMQReceiver&) = delete;

        void start(std::unique_ptr<util::ZMQInstance> sub) {
            _sub = std::move(sub);
            _thread = std::make_unique<std::thread>(run, this);
        }

    private:
        std::unique_ptr<std::thread> _thread;
        std::unique_ptr<util::ZMQInstance> _sub;
        std::function<void(std::string raw)> _cb;
    };

    // LocalDistributor is responsible for broadcasting messages received from raft to other local machines
    class LocalDistributor {
    public:
        // Input: port of all local servers
        // output: decisions
        static std::unique_ptr<LocalDistributor> NewLocalDistributor(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& nodes, int myPos) {
            auto ld = std::make_unique<LocalDistributor>();
            if (nodes.empty()) {
                return ld;
            }
            // connect to those servers
            auto pub = util::ZMQInstance::NewServer<zmq::socket_type::pub>(nodes[myPos]->port);
            if (pub == nullptr) {
                return nullptr;
            }
            ld->_pub = std::move(pub);
            ld->_subs.reserve(nodes.size()-1);  // skip local sub
            auto cb = [ptr = ld.get()](std::string raw) {    // receive function
                ptr->_deliverCallback(std::move(raw));
            };
            for (int i=0; i<(int)nodes.size(); i++) {
                if (i == myPos) {
                    continue;   // skip local
                }
                auto sub = util::ZMQInstance::NewClient<zmq::socket_type::sub>(nodes[i]->priAddr(), nodes[i]->port);
                if (sub == nullptr) {
                    return nullptr;
                }
                auto azr = std::make_unique<ActiveZMQReceiver>(cb);
                azr->start(std::move(sub));
                ld->_subs.push_back(std::move(azr));
            }
            return ld;
        }

        // gossip should be thread safe
        inline bool gossip(std::string&& msg) {
            _deliverCallback(std::string(msg));
            if (_pub == nullptr) {
                return true;
            }
            std::unique_lock guard(gossipMutex);
            return _pub->send(std::move(msg));
        }

        inline bool gossip(const std::string& msg) {
            _deliverCallback(std::string(msg.data(), msg.size()));
            if (_pub == nullptr) {
                return true;
            }
            std::unique_lock guard(gossipMutex);
            return _pub->send(msg);
        }

        // the delivery callback is called concurrently by multiple receiver clients
        void setDeliverCallback(auto&& cb) { _deliverCallback = std::forward<decltype(cb)>(cb); }

    private:
        std::mutex gossipMutex;
        std::unique_ptr<util::ZMQInstance> _pub;
        std::vector<std::unique_ptr<ActiveZMQReceiver>> _subs;
        std::function<void(std::string msg)> _deliverCallback;
    };

    class RaftCallback {
    public:
        virtual ~RaftCallback() = default;

        // Called after receiving a message from raft, responsible for broadcasting to all local nodes
        inline bool onBroadcast(std::string decision) { return _localDistributor->gossip(std::move(decision)); }

        // Called when the remote leader is down
        inline void onError(int subChainId) {
            if (onErrorHandle) {
                onErrorHandle(subChainId);
            }
        }

        // Called after receiving AppendEntries from raft (as a follower)
        inline bool onValidate(std::string decision) { return onValidateHandle(std::move(decision)); }

        // Called on return after determining the final order of sub chain blocks
        inline bool onExecuteBlock(int subChainId, int blockId) { return onExecuteBlockHandle(subChainId, blockId); }

    public:
        void setOnErrorCallback(auto&& cb) { onErrorHandle = std::forward<decltype(cb)>(cb); }

        void setOnBroadcastCallback(auto&& cb) { onBroadcastHandle = std::forward<decltype(cb)>(cb); }

        void setOnValidateCallback(auto&& cb) { onValidateHandle = std::forward<decltype(cb)>(cb); }

        void setOnExecuteBlockCallback(auto&& cb) { onExecuteBlockHandle = std::forward<decltype(cb)>(cb); }

    public:
        virtual void init(int, std::unique_ptr<LocalDistributor> ld) {
            _localDistributor = std::move(ld);
            _localDistributor->setDeliverCallback([&](std::string decision) {
                if (!onBroadcastHandle(std::move(decision))) {
                    LOG(WARNING) << "receive wrong order from gossip!";
                }
            });
        }

    private:
        std::function<void(int subChainId)> onErrorHandle;

        std::function<bool(std::string decision)> onBroadcastHandle;

        std::function<bool(std::string decision)> onValidateHandle;

        std::function<bool(int subChainId, int blockId)> onExecuteBlockHandle;

        std::unique_ptr<v2::LocalDistributor> _localDistributor;
    };

}
