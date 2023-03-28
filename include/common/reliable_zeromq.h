//
// Created by peng on 2/14/23.
//

#pragma once

#include "common/zeromq.h"
#include "common/meta_rpc_server.h"
#include "brpc/server.h"
#include "brpc/channel.h"
#include "bthread/countdown_event.h"
#include "proto/zeromq.pb.h"
#include <thread>
#include <memory>
#include <shared_mutex>

namespace util {
    // Push-based reliable zmq connection between a client and a server
    class ReliableZmqServer {
    public:
        constexpr static const auto HELLO_MESSAGE = "hello!";

        ~ReliableZmqServer() {
            shutdown();
            if (_helloThread) { _helloThread->join(); }
        }

        inline std::optional<zmq::message_t> receive() {
            if (firstInvokeReceive) {
                firstInvokeReceive = false;
                return waitReady();
            }
            return receiver->receive();
        }

        void shutdown() {
            if (receiver) { receiver->shutdown(); }
            receivedHello.reset(1);
            receivedHello.signal(1);
        }

        // When receive garbage, try this one
        inline std::optional<zmq::message_t> waitReady() {
            while(receivedHello.wait() != 0);
            while(true) {
                auto data=receiver->receive();
                if (data != std::nullopt && data->to_string_view() != HELLO_MESSAGE) {
                    return data;
                }
                if (data == std::nullopt) {
                    return data;
                }
            }
        }

    protected:
        ReliableZmqServer() : isReady(false) { }

        template<std::array addrType=std::to_array("tcp")>
        int initServer(int port) {
            receiver = util::ZMQInstance::NewServer<zmq::socket_type::sub, addrType>(port);
            if (receiver == nullptr) {
                return -1;
            }
            _helloThread = std::make_unique<std::thread>(wait_hello, this);
            return 0;
        }

    public:
        // not thread safe, invoke BEFORE all operations
        static int AddRPCService() {
            auto* service = new ZmqControlServiceImpl();
            // Add services into server. Notice the second parameter, because the
            // service is put on stack, we don't want server to delete it, otherwise use brpc::SERVER_OWNS_SERVICE.
            if (MetaRpcServer::AddService(service, []{ globalControlService = nullptr; }) != 0) {
                LOG(ERROR) << "Fail to add globalControlService!";
                return -1;
            }
            globalControlService = service;
            return 0;
        }

        // just create a sub server
        static bool NewSubscribeServer(int port) {
            if (globalControlService == nullptr) {
                LOG(ERROR) << "GlobalControlService is not inited!";
                return false;
            }
            return globalControlService->newConnection(port);
        }

        static std::shared_ptr<ReliableZmqServer> GetSubscribeServer(int port) {
            auto deleter = [port=port](auto*) {
                PutSubscribeServer(port);
            };
            if (globalControlService == nullptr) {
                LOG(ERROR) << "GlobalControlService is not inited!";
                return nullptr;
            }
            return {globalControlService->getReliableZmqServer(port), deleter};
        }

        static bool DestroySubscribeServer(int port) {
            if (globalControlService == nullptr) {
                LOG(ERROR) << "GlobalControlService is not inited!";
                return false;
            }
            return globalControlService->dropConnection(port);
        }

    protected:
        static void PutSubscribeServer(int port) {
            if (globalControlService != nullptr) {
                globalControlService->restoreReliableZmqServer(port);
            }
        }

        static void* wait_hello(void* ptr) {
            auto* serverPtr = static_cast<ReliableZmqServer*>(ptr);
            if (auto helloMsg = serverPtr->receiver->receive(); helloMsg != std::nullopt) {
                if (helloMsg->to_string_view() == HELLO_MESSAGE) {
                    serverPtr->isReady = true;          // to alert the rpc service
                    serverPtr->receivedHello.signal();  // to alert the local server
                }
            }
            return nullptr;
        }

    private:
        // if receive hello from remote client, isReady=true
        std::unique_ptr<std::thread> _helloThread;
        std::atomic<bool> isReady;
        bool firstInvokeReceive = true;
        bthread::CountdownEvent receivedHello;
        std::unique_ptr<ZMQInstance> receiver;

        class ZmqControlServiceImpl : public util::ZmqControlService {
        public:
            void newConnection(google::protobuf::RpcController*,
                               const ::util::ZmqControlRequest* request,
                               ::util::ZmqControlResponse* response,
                               ::google::protobuf::Closure* done) override {
                brpc::ClosureGuard guard(done);
                do {
                    auto port = (int)request->port();
                    if (mutex[port].try_lock()) {
                        std::unique_lock m_guard(mutex[port], std::adopt_lock);
                        // auto* ctl = static_cast<brpc::Controller*>(controller);
                        // ctl->set_response_compress_type(brpc::COMPRESS_TYPE_GZIP);
                        if (zmqServerList[port] != nullptr) {
                            break;  // server exist
                        }
                        std::unique_ptr<ReliableZmqServer> server(new ReliableZmqServer());
                        if (server->initServer(port) != 0) {
                            break;  // init failed
                        }

                        zmqServerList[port] = std::move(server);
                        response->set_success(true);
                        return;
                    }
                } while(false);
                response->set_success(false);
            }

            void hello(google::protobuf::RpcController*,
                       const ::util::ZmqControlRequest* request,
                       ::util::ZmqControlResponse* response,
                       ::google::protobuf::Closure* done) override {
                brpc::ClosureGuard guard(done);
                do {
                    auto port = (int) request->port();
                    if (mutex[port].try_lock_shared()) {
                        std::shared_lock m_guard(mutex[port], std::adopt_lock);
                        if (zmqServerList[port] == nullptr) {
                            break; // server not exist
                        }
                        auto *server = zmqServerList[port].get();
                        response->set_success(server->isReady);
                        return;
                    }
                } while(false);
                response->set_success(false); // lock failure
            }

            void dropConnection(google::protobuf::RpcController*,
                                const ::util::ZmqControlRequest* request,
                                ::util::ZmqControlResponse* response,
                                ::google::protobuf::Closure* done) override {
                brpc::ClosureGuard guard(done);
                do {
                    auto port = (int) request->port();
                    if (mutex[port].try_lock()) {
                        std::unique_lock m_guard(mutex[port], std::adopt_lock);
                        if (zmqServerList[port] == nullptr) {
                            break;  // server not exist
                        }
                        zmqServerList[port].reset();
                        response->set_success(true);
                        return;
                    }
                } while(false);
                response->set_success(false); // lock failure
            }
        public:
            bool newConnection(int port) {
                if (mutex[port].try_lock()) {
                    std::unique_lock m_guard(mutex[port], std::adopt_lock);
                    if (zmqServerList[port] != nullptr) {
                        return false;
                    }
                    std::unique_ptr<ReliableZmqServer> server(new ReliableZmqServer());
                    if (server->initServer(port) != 0) {
                        return false;
                    }
                    zmqServerList[port] = std::move(server);
                    return true;
                }
                return false;
            }

            bool dropConnection(int port) {
                if (mutex[port].try_lock()) {
                    std::unique_lock m_guard(mutex[port], std::adopt_lock);
                    if (zmqServerList[port] == nullptr) {
                        return false;
                    }
                    zmqServerList[port].reset();
                    return true;
                }
                return false;
            }

            ReliableZmqServer* getReliableZmqServer(int port) {
                if (mutex[port].try_lock_shared()) {
                    if (zmqServerList[port] == nullptr) {
                        return nullptr;
                    }
                    return zmqServerList[port].get();
                }
                return nullptr; // in use, retry later
            }

            void restoreReliableZmqServer(int port) {
                mutex[port].unlock_shared();
            }

        private:
            std::array<std::shared_mutex, 65536> mutex;
            std::array<std::unique_ptr<ReliableZmqServer>, 65536> zmqServerList;
        };

        // globalControlServer own globalControlService
        inline static ZmqControlServiceImpl* globalControlService = nullptr;
    };


    class ReliableZmqClient {
    public:
        template<class CT=std::string>
        inline auto send(CT&& msg) {
            return client->send(std::forward<CT>(msg));
        }

    public:
        static std::unique_ptr<ReliableZmqClient> NewPublishClient(const std::string& ip, int port, int rpcPort = 9500, int retry=5, int timeout_ms=50) {
            std::unique_ptr<ReliableZmqClient> rClient(new ReliableZmqClient);
            rClient->client = util::ZMQInstance::NewClient<zmq::socket_type::pub>(ip, port);
            if (rClient->client == nullptr) {
                return nullptr;
            }
            // A Channel represents a communication line to a Server. Notice that
            // Channel is thread-safe and can be shared by all threads in your program.
            brpc::Channel channel;
            // Initialize the channel, NULL means using default options.
            brpc::ChannelOptions options;
            options.protocol = "h2:grpc";
            options.timeout_ms = 200 /*milliseconds*/;
            options.max_retry = 0;
            if (channel.Init(ip.data(), rpcPort, &options) != 0) {
                LOG(ERROR) << "Fail to initialize channel";
                return nullptr;
            }
            // Normally, you should not call a Channel directly, but instead construct
            // a stub Service wrapping it. stub can be shared by all threads as well.
            util::ZmqControlService_Stub stub(&channel);

            // Send a request and wait for the response every 1 second.
            for(int i=0; i<retry; i++) {
                rClient->send(std::string(ReliableZmqServer::HELLO_MESSAGE));
                // We will receive response synchronously, safe to put variables
                // on stack.
                util::ZmqControlRequest request;
                util::ZmqControlResponse response;
                brpc::Controller ctl;
                // ctl.set_request_compress_type(brpc::COMPRESS_TYPE_GZIP);
                request.set_port(port);

                // Because `done'(last parameter) is NULL, this function waits until
                // the response comes back or error occurs(including timedout).
                stub.hello(&ctl, &request, &response, nullptr);
                if (!ctl.Failed()) {
                    DLOG(INFO) << "Received response from " << ctl.remote_side()
                               << " to " << ctl.local_side()
                               << ": " << response.success()
                               << " latency=" << ctl.latency_us() << "us";
                    if (response.success()) {
                        return rClient;
                    }
                } else {
                    LOG(WARNING) << ctl.ErrorText();
                }
                usleep(timeout_ms*1000);
            }
            LOG(ERROR) << "Failed to connect to remote server!";
            return nullptr;
        }

    private:
        std::unique_ptr<ZMQInstance> client;
    };
}