//
// Created by peng on 11/27/22.
//

#pragma once

#include <string>
#include <optional>
#include <zmq.hpp>
#include "glog/logging.h"

namespace util {
    class ZMQInstance {
    public:
        ~ZMQInstance() = default;

        ZMQInstance(const ZMQInstance&) = delete;

        // The difference is that a PUB socket sends the same message to all subscribers,
        // whereas PUSH does a round-robin amongst all its connected PULL sockets.
        // USE ANOTHER SOCKET TO CONTROL THIS SOCKET CONNECTION
        template<zmq::socket_type socketType, std::array addrType=std::to_array("tcp")>
        requires std::same_as<typename decltype(addrType)::value_type, char>
        static std::unique_ptr<ZMQInstance> NewClient(const std::string& ip, int port) {
            auto ctx = std::make_unique<zmq::context_t>();
            auto socket = std::make_unique<zmq::socket_t>(*ctx, socketType);
            if (socketType == zmq::socket_type::sub || socketType == zmq::socket_type::xsub){
                socket->set(zmq::sockopt::subscribe, "");
                socket->set(zmq::sockopt::rcvhwm, 0);
            }
            if (socketType == zmq::socket_type::pull) {
                socket->set(zmq::sockopt::rcvhwm, 0);
            }
            if (socketType == zmq::socket_type::pub || socketType == zmq::socket_type::xpub){
                socket->set(zmq::sockopt::sndhwm, 0);
            }
            try {
                auto addr = std::string(addrType.data()) + "://"+ ip +":" + std::to_string(port);
                DLOG(INFO) << "Connect to address: " << addr;
                socket->connect(addr);
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "Creating ZMQ instance failed, " << error.what();
                return nullptr;
            }
            return std::unique_ptr<ZMQInstance>(new ZMQInstance(std::move(ctx), std::move(socket)));
        }

        template<zmq::socket_type socketType, std::array addrType=std::to_array("tcp")>
        requires std::same_as<typename decltype(addrType)::value_type, char>
        static std::unique_ptr<ZMQInstance> NewServer(int port) {
            auto ctx = std::make_unique<zmq::context_t>();
            auto socket = std::make_unique<zmq::socket_t>(*ctx, socketType);
            if (socketType == zmq::socket_type::sub || socketType == zmq::socket_type::xsub){
                socket->set(zmq::sockopt::subscribe, "");
                socket->set(zmq::sockopt::rcvhwm, 0);
            }
            if (socketType == zmq::socket_type::push) {
                socket->set(zmq::sockopt::sndhwm, 0);
            }
            if (socketType == zmq::socket_type::pub || socketType == zmq::socket_type::xpub){
                socket->set(zmq::sockopt::sndhwm, 0);
            }
            try {
                auto addr = std::string(addrType.data()) + "://0.0.0.0:" + std::to_string(port);
                DLOG(INFO) << "Listening at address: " << addr;
                socket->bind(addr);
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "Creating ZMQ instance failed, " << error.what();
                return nullptr;
            }
            return std::unique_ptr<ZMQInstance>(new ZMQInstance(std::move(ctx), std::move(socket)));
        }

        // deserialize the data
        std::optional<zmq::message_t> receive() {
            zmq::message_t msg;
            try {
                while (true) {
                    auto res = _socket->recv(msg, zmq::recv_flags::none);
                    if (res != std::nullopt) {
                        return msg;
                    }
                }
            } catch (const zmq::error_t& error) {
                LOG(INFO) << "ZMQ instance receive message failed, " << error.what();
                return std::nullopt;
            }
        }

        void shutdown() { _context->shutdown(); }

        // Zero copy is only available for sender
        template<class CT=std::string>
        auto send(CT&& msg) {
            using CTNoCVR = std::remove_cvref<CT>::type;
            auto* container = new CTNoCVR(std::forward<CT>(msg));
            zmq::message_t zmqMsg(static_cast<void *>(container->data()), container->size(), freeBufferCallback<CTNoCVR>, nullptr);
            return sendInternal(zmqMsg);
        }

    protected:
        ZMQInstance(auto context, auto socket)
                :_context(std::move(context)), _socket(std::move(socket)){ }

        bool sendInternal(zmq::message_t& msg) {
            try {
                while (true) {
                    auto res = _socket->send(msg, zmq::send_flags::none);
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
        std::unique_ptr<zmq::context_t> _context;
        std::unique_ptr<zmq::socket_t> _socket;
    };
}