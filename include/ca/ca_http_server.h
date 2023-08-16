//
// Created by user on 23-8-8.
//

#include "nlohmann/json.hpp"

namespace httplib {
    class Server;
}

namespace ca {
    class ServiceBackend;

    class ServerBackend {
    public:
        ~ServerBackend();

        static std::unique_ptr<ServerBackend> NewServerBackend(std::unique_ptr<ServiceBackend> service,
                                                               std::shared_ptr<httplib::Server> httpServer);

    protected:
        ServerBackend() = default;

        void initRoutes();

        bool addNode(const nlohmann::json& json);

    private:
        std::shared_ptr<httplib::Server> _server;
        std::unique_ptr<ServiceBackend> _service;
    };
}
