//
// Created by peng on 11/6/22.
//

#include "client/crdt/crdt_engine.h"

int main(int argc, char *argv[]) {
    util::OpenSSLSHA256::initCrypto();
    util::OpenSSLED25519::initCrypto();
    util::Properties::LoadProperties("peer.yaml");

    auto* p = util::Properties::GetProperties();
    client::crdt::CrdtEngine engine(*p);
    engine.startTest();
    return 0;
}