//
// Created by peng on 11/27/22.
//

#pragma once

#include <string>
#include <optional>
#include <zmq.hpp>
#include "glog/logging.h"
#include "riften/deque.hpp"

namespace util {
    class ZMQInstance {
    public:
        ~ZMQInstance() = default;

        ZMQInstance(const ZMQInstance&) = delete;

        template<zmq::socket_type socketType>
        static std::unique_ptr<ZMQInstance> NewClient(const std::string& ip, int port) {
            zmq::context_t ctx;
            zmq::socket_t socket(ctx, socketType);
            if (socketType == zmq::socket_type::sub || socketType == zmq::socket_type::xsub){
                socket.set(zmq::sockopt::subscribe, "");
                socket.set(zmq::sockopt::rcvhwm, 0);
            }
            try {
                auto addr = "tcp://"+ ip +":" + std::to_string(port);
                DLOG(INFO) << "Connect to address: " << addr;
                socket.connect(addr);
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "Creating ZMQ instance failed, " << error.what();
                return nullptr;
            }
        }

        template<zmq::socket_type socketType>
        static std::unique_ptr<ZMQInstance> NewServer(int port) {
            zmq::context_t ctx;
            zmq::socket_t socket(ctx, socketType);
            if (socketType == zmq::socket_type::sub || socketType == zmq::socket_type::xsub){
                socket.set(zmq::sockopt::subscribe, "");
                socket.set(zmq::sockopt::rcvhwm, 0);
            }
            try {
                auto addr = "tcp://0.0.0.0:" + std::to_string(port);
                DLOG(INFO) << "Listening at address: " << addr;
                socket.bind(addr);
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "Creating ZMQ instance failed, " << error.what();
                return nullptr;
            }
        }

        // deserialize the data
        std::optional<zmq::message_t> receive() {
            zmq::message_t msg;
            try {
                while (true) {
                    auto res = _socket.recv(msg, zmq::recv_flags::none);
                    if (res != std::nullopt) {
                        return msg;
                    }
                }
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "ZMQ instance receive message failed, " << error.what();
                return std::nullopt;
            }
        }

        // Zero copy is only available for sender
        template<class CT=std::string>
        requires requires (CT x) { x.data(); x.size(); CT(std::forward<CT>(x)); }
        auto send(CT&& msg) {
            auto* container = new CT(std::forward<CT>(msg));
            zmq::message_t zmqMsg(container->data(), container->size(), freeBufferCallback<CT>, nullptr);
            return sendInternal(zmqMsg);
        }

    protected:
        ZMQInstance(zmq::context_t&& context, zmq::socket_t&& socket)
                :_context(std::move(context)), _socket(std::move(socket)){ }

        bool sendInternal(zmq::message_t& msg) {
            try {
                while (true) {
                    auto res = _socket.send(msg, zmq::send_flags::none);
                    if (res != std::nullopt) {
                        return true;
                    }
                }
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "ZMQ instance send message failed, " << error.what();
                return false;
            }
        }

    private:
        template<class T>
        static void freeBufferCallback(void*, void *hint) {
            auto* container = static_cast<T*>(hint);
            delete container;
        }
        zmq::context_t&& _context;
        zmq::socket_t&& _socket;
    };
}