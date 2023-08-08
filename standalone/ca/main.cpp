//
// Created by user on 23-7-4.
//

#include "ca/ca_http_service.h"
#include "ca/ca_http_server.h"
#include "httplib.h"
#include "glog/logging.h"

int main(int argc, char *argv[]) {
    auto dispatcher = std::make_unique<ca::Dispatcher>("/home/user", "nc_bft", "ncp");
    auto service = ca::ServiceBackend::NewServiceBackend(std::move(dispatcher));
    if (!service) {
        return -1;
    }
    auto httpServer = std::make_shared<httplib::Server>();
    auto server = ca::ServerBackend::NewServerBackend(std::move(service), httpServer);
    if (!server) {
        return -1;
    }
    if (!httpServer->listen("0.0.0.0", 8081)) {
        return -1;
    }
    return 0;
}