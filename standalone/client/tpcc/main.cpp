//
// Created by peng on 11/6/22.
//

#include "client/tpcc/tpcc_engine.h"

int main(int argc, char *argv[]) {
    util::OpenSSLSHA256::initCrypto();
    util::OpenSSLED25519::initCrypto();
    util::Properties::LoadProperties("peer.yaml");

//    client::tpcc::TPCCProperties::SetTPCCProperties(client::tpcc::TPCCProperties::RANDOM_SEED, false);
//    client::tpcc::TPCCProperties::SetTPCCProperties(client::tpcc::TPCCProperties::THREAD_COUNT_PROPERTY, 1);
//    client::tpcc::TPCCProperties::SetTPCCProperties(client::tpcc::TPCCProperties::PAYMENT_PROPORTION_PROPERTY, 1.0);

    auto* p = util::Properties::GetProperties();
    client::tpcc::TPCCEngine engine(*p);
    engine.startTest();
    return 0;
}