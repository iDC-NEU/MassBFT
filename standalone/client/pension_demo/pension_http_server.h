//
// Created by user on 23-8-8.
//

#pragma once

#include "common/crypto.h"
#include "client/core/status.h"

namespace client::sdk {
    class ClientSDK;
}

namespace proto {
    class Envelop;
}

namespace httplib {
    class Server;
}

namespace client::sdk {
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
        client::core::Status put(const std::string& key, const std::string& value);

        client::core::Status putDigest(const std::string& key, const util::OpenSSLSHA256::digestType& digest);

        client::core::Status getDigest(const std::string& key);

        client::sdk::ReceiveInterface* getReceiver();

        const util::Properties& getProperties() { return *_prop; }

    protected:
        ServiceBackend() = default;

    private:
        std::unique_ptr<client::sdk::ClientSDK> _sdk;
        std::shared_ptr<util::Properties> _prop;
    };

    class ServerController;

    class ServerBackend {
    public:
        ~ServerBackend();

        static std::unique_ptr<ServerBackend> NewServerBackend(std::unique_ptr<ServiceBackend> service,
                                                               std::shared_ptr<httplib::Server> httpServer);

    protected:
        ServerBackend() = default;

    private:
        std::shared_ptr<httplib::Server> _server;

        std::unique_ptr<ServerController> _controller;
    };
}