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
                instance->_cb(std::move(*ret));
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
        std::function<void(zmq::message_t&& raw)> _cb;
    };

    // LocalDistributor is responsible for broadcasting messages received from raft to other local machines
    class LocalDistributor {
    public:
        // Input: port of all local servers
        // output: decisions
        static std::unique_ptr<LocalDistributor> NewLocalDistributor(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& nodes, int myPos) {
            // connect to those servers
            auto pub = util::ZMQInstance::NewServer<zmq::socket_type::pub>(nodes[myPos]->port);
            if (pub == nullptr) {
                return nullptr;
            }
            auto ld = std::make_unique<LocalDistributor>();
            ld->_pub = std::move(pub);
            ld->_subs.reserve(nodes.size()-1);  // skip local sub
            auto cb = [ptr = ld.get()](auto&& raw) {    // receive function
                ptr->_deliverCallback(std::forward<decltype(raw)>(raw));
            };
            for (int i=0; i<(int)nodes.size(); i++) {
                if (i == myPos) {
                    continue;   // skip local
                }
                auto sub = util::ZMQInstance::NewClient<zmq::socket_type::sub>(nodes[i]->addr(), nodes[i]->port);
                if (sub == nullptr) {
                    return nullptr;
                }
                auto azr = std::make_unique<ActiveZMQReceiver>(cb);
                azr->start(std::move(sub));
                ld->_subs.push_back(std::move(azr));
            }
            return ld;
        }

        bool gossip(const std::string& msg) {
            _deliverCallback(zmq::message_t(msg));
            return _pub->send(std::forward<decltype(msg)>(msg));
        }

        // the delivery callback is called concurrently by multiple receiver clients
        void setDeliverCallback(auto&& cb) { _deliverCallback = std::forward<decltype(cb)>(cb); }

    private:
        std::unique_ptr<util::ZMQInstance> _pub;
        std::vector<std::unique_ptr<ActiveZMQReceiver>> _subs;
        std::function<void(zmq::message_t msg)> _deliverCallback;
    };

}
