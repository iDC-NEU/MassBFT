//
// Created by user on 23-7-4.
//

#include "ca/config_initializer.h"
#include "glog/logging.h"

int main(int argc, char *argv[]) {
    ca::Initializer i({1});
    i.initDefaultConfig();
    ca::Initializer::SetLocalId(0, 1);
    // ca::Initializer::SetNodeIp(1, 5, "127.0.0.1", "127.0.0.1");
    ca::Initializer::SaveConfig("peer.yaml");

    auto d = ca::Dispatcher("/home/user", "nc_bft", "ncp");
    d.transmitFileToRemote("47.92.107.184");
    return 0;
}