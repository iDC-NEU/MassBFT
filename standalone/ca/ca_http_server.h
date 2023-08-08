//
// Created by user on 23-8-8.
//

#include <memory>

namespace httplib {
    class Server;
}

namespace ca {
    class ServiceBackend;

    class ServerBackend {
    public:
        ~ServerBackend();

        static std::unique_ptr<ServerBackend> NewServerBackend(std::unique_ptr<ServiceBackend> service);

        bool start(int port);

    protected:
        ServerBackend() = default;

    private:
        std::unique_ptr<httplib::Server> _server;
        std::unique_ptr<ServiceBackend> _service;
    };
}
