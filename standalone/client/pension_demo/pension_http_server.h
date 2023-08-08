//
// Created by user on 23-8-8.
//

#pragma once

#include "common/crypto.h"

namespace ycsb::sdk {
    class ClientSDK;
}

namespace proto {
    class Envelop;
}

namespace httplib {
    class Server;
}

namespace ycsb::sdk {
    class ReceiveInterface;
}

namespace util {
    class Properties;
}

namespace demo::pension {
    class ServiceBackend {
    public:
        static std::unique_ptr<ServiceBackend> NewServiceBackend(std::shared_ptr<util::Properties> prop);

        ~ServiceBackend();

    public:
        std::unique_ptr<proto::Envelop> put(const std::string& key, const std::string& value);

        std::unique_ptr<proto::Envelop> putDigest(const std::string& key, const util::OpenSSLSHA256::digestType& digest);

        std::unique_ptr<proto::Envelop> getDigest(const std::string& key);

        ycsb::sdk::ReceiveInterface* getReceiver();

        const util::Properties& getProperties() { return *_prop; }

    protected:
        ServiceBackend() = default;

    private:
        std::unique_ptr<ycsb::sdk::ClientSDK> _sdk;
        std::shared_ptr<util::Properties> _prop;
    };

    class ServerController;

    class ServerBackend {
    public:
        ~ServerBackend();

        static std::unique_ptr<ServerBackend> NewServerBackend(std::unique_ptr<ServiceBackend> service);

        bool start(int port);

    protected:
        ServerBackend() = default;

    private:
        std::unique_ptr<httplib::Server> _server;

        std::unique_ptr<ServerController> _controller;
    };
}