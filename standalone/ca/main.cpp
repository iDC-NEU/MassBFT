//
// Created by user on 23-7-4.
//

#include "ca_http_service.h"
#include "ca_http_server.h"
#include "common/ssh.h"
#include "glog/logging.h"

int main(int argc, char *argv[]) {
    auto dispatcher = std::make_unique<ca::Dispatcher>("/home/user", "nc_bft", "ncp");
    auto service = ca::ServiceBackend::NewServiceBackend(std::move(dispatcher));
    if (!service) {
        return -1;
    }
    auto server = ca::ServerBackend::NewServerBackend(std::move(service));
    if (!server) {
        return -1;
    }
    if (!server->start(8082)) {
        return -1;
    }
    return 0;
}