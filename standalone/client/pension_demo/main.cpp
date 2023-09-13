//
// Created by user on 23-8-8.
//

#include "pension_http_server.h"
#include "common/property.h"
#include "httplib.h"

int main(int argc, char *argv[]) {
    if (!util::Properties::LoadProperties("peer.yaml")) {
        LOG(ERROR) << "Load config failed!";
        return -1;
    }
    auto service = demo::pension::ServiceBackend::NewServiceBackend(util::Properties::GetSharedProperties());
    if (service == nullptr) {
        return -1;
    }
    auto httpServer = std::make_shared<httplib::Server>();
    auto server = demo::pension::ServerBackend::NewServerBackend(std::move(service), httpServer);
    if (server == nullptr) {
        return -1;
    }
    if (!httpServer->listen("0.0.0.0", 8081)) {
        return -1;
    }
    return 0;
}