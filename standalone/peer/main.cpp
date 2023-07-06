//
// Created by peng on 11/6/22.
//

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

    ~PeerInstance() {
        util::MetaRpcServer::Stop();

    }
};

int main(int argc, char *argv[]) {
    return 0;
}