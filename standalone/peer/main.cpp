//
// Created by peng on 11/6/22.
//

#include "peer/core/module_coordinator.h"
#include "common/property.h"
#include "common/crypto.h"
#include "common/reliable_zeromq.h"
#include "common/cmd_arg_parser.h"

class PeerInstance {
public:
    PeerInstance() {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    }

    bool initInstance() {
        _mc = peer::core::ModuleCoordinator::NewModuleCoordinator(util::Properties::GetSharedProperties());
        if (_mc == nullptr) {
            return false;
        }
        return true;
    }

    bool startInstance() {
        if (!_mc->startInstance()) {
            return false;
        }
        _mc->waitInstanceReady();
        return true;
    }

    bool initDB(const std::string& ccName) {
        if (_mc->initChaincodeData(ccName)) {
            return true;
        }
        if (_mc->initCrdtChaincodeData(ccName)) {
            return true;
        }
        return false;
    }

    ~PeerInstance() {
        util::MetaRpcServer::Stop();
    }

protected:
    std::unique_ptr<peer::core::ModuleCoordinator> _mc;
};

void OverrideLocalNodeInfo(const util::ArgParser& argParser) {
    auto* p = util::Properties::GetProperties();
    auto n = p->getNodeProperties();
    auto localNode = n.getLocalNodeInfo();
    try {
        if (auto nodeId = argParser.getOption("-n"); nodeId != std::nullopt) {
            localNode->nodeId = std::stoi(*nodeId);
        }
        if (auto groupId = argParser.getOption("-g"); groupId != std::nullopt) {
            localNode->groupId = std::stoi(*groupId);
        }
    } catch(...) {
        LOG(ERROR) << "Load input params error";
    }
    n.setLocalNodeInfo(localNode->groupId, localNode->nodeId);
}

/*
 * Usage:
 *  default: transaction mode.
 *  -n = [node_id]: override local node id
 *  -g = [group_id]: override local group id
 *  -i = [chaincode_name]: init chaincode data of chaincode_name.
 */
int main(int argc, char *argv[]) {
    util::ArgParser argParser(argc, argv);
    util::Properties::LoadProperties("peer.yaml");
    // override node id and group id
    OverrideLocalNodeInfo(argParser);

    auto peer = PeerInstance();
    if (!peer.initInstance()) {
        return -1;
    }
    // init database
    if (auto ccName = argParser.getOption("-i"); ccName != std::nullopt) {
        LOG(INFO) << "Init db for chaincode: " << *ccName << " .";
        if (!peer.initDB(*ccName)) {
            return -1;
        }
        LOG(INFO) << "Init db for chaincode: " << *ccName << " completed.";
        if (!peer::db::IsDBHashMap()) {
            return 0;
        }
        LOG(INFO) << "Using hashmap as db, continue starting peer.";
    }
    // startup
    if (!peer.startInstance()) {
        return -1;
    }
    LOG(INFO) << "This peer is started.";
    while (!brpc::IsAskedToQuit()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(INFO) << "Peer is quitting...";
    return 0;
}