//
// Created by user on 23-7-4.
//

#include "ca/ca_http_service.h"
#include "ca/ca_http_server.h"
#include "common/cmd_arg_parser.h"
#include "httplib.h"
#include "glog/logging.h"

/*
 * Usage:
 *  -r = [runningPath]: override runningPath
 *  -b = [bftFolderName]: override bftFolderName
 *  -n = [ncZipFolderName]: override ncZipFolderName
 */
int main(int argc, char *argv[]) {
    std::filesystem::path runningPath = "/home/user";
    std::string bftFolderName = "nc_bft";
    std::string ncZipFolderName = "ncp";

    util::ArgParser argParser(argc, argv);
    try {
        if (auto ret = argParser.getOption("-r"); ret != std::nullopt) {
            runningPath = *ret;
            LOG(INFO) << "Override runningPath to: " << runningPath;
        }
        if (auto ret = argParser.getOption("-b"); ret != std::nullopt) {
            bftFolderName = *ret;
            LOG(INFO) << "Override bftFolderName to: " << bftFolderName;
        }
        if (auto ret = argParser.getOption("-n"); ret != std::nullopt) {
            ncZipFolderName = *ret;
            LOG(INFO) << "Override ncZipFolderName to: " << ncZipFolderName;
        }
    } catch(...) {
        LOG(ERROR) << "Load input params error";
    }

    auto dispatcher = std::make_unique<ca::Dispatcher>(runningPath, bftFolderName, ncZipFolderName);
    auto service = ca::ServiceBackend::NewServiceBackend(std::move(dispatcher));
    if (!service) {
        return -1;
    }
    auto httpServer = std::make_shared<httplib::Server>();
    auto server = ca::ServerBackend::NewServerBackend(std::move(service), httpServer);
    if (!server) {
        return -1;
    }
    LOG(INFO) << "CA backend is listening on 0.0.0.0:8081";
    if (!httpServer->listen("0.0.0.0", 8081)) {
        return -1;
    }
    return 0;
}