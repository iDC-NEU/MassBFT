//
// Created by peng on 11/6/22.
//

#include "peer/core/module_coordinator.h"
#include "common/property.h"
#include "common/crypto.h"
#include "common/reliable_zeromq.h"

class PeerInstance {
public:
    PeerInstance() {
        util::OpenSSLSHA256::initCrypto();
        util::OpenSSLED25519::initCrypto();
        util::ReliableZmqServer::AddRPCService();
        util::MetaRpcServer::Start();
    }

    bool initInstance() {
        auto mc = peer::core::ModuleCoordinator::NewModuleCoordinator(util::Properties::GetSharedProperties());
        if (mc == nullptr) {
            return false;
        }
        if (!mc->startInstance()) {
            return false;
        }
        mc->waitInstanceReady();
        _mc = std::move(mc);
        return true;
    }

    ~PeerInstance() {
        util::MetaRpcServer::Stop();
    }

protected:
    std::unique_ptr<peer::core::ModuleCoordinator> _mc;
};

int main(int argc, char *argv[]) {
    util::Properties::LoadProperties("peer.yaml");
    auto peer = PeerInstance();
    if (!peer.initInstance()) {
        return -1;
    }
    while (!brpc::IsAskedToQuit()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    LOG(INFO) << "Peer is quitting...";
    return 0;
}