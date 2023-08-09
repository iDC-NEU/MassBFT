//
// Created by user on 23-7-12.
//

#pragma once

#include "common/zeromq.h"
#include <mutex>

namespace client {
    class NeuChainDBConnection {
    public:
        static std::shared_ptr<NeuChainDBConnection> NewNeuChainDBConnection(const std::string& ip, int port) {
            std::shared_ptr<NeuChainDBConnection> dbc(new NeuChainDBConnection);
            dbc->_invokeClient = util::ZMQInstance::NewClient<zmq::socket_type::pub>(ip, port);
            if (!dbc->_invokeClient) {
                return nullptr;
            }
            return dbc;
        }

        void shutdown() {
            if (_invokeClient) {
                _invokeClient->shutdown();
            }
        }

        auto send(auto&& msg) {
            std::unique_lock lock(_mutex);
            return _invokeClient->send(std::forward<decltype(msg)>(msg));
        }

    protected:
        NeuChainDBConnection() = default;

    private:
        std::mutex _mutex;
        std::unique_ptr<util::ZMQInstance> _invokeClient;
    };
}