//
// Created by user on 23-7-4.
//

#include "ca/config_initializer.h"
#include "common/ssh.h"
#include "glog/logging.h"

int main(int argc, char *argv[]) {
    ca::Initializer initializer({3});
    initializer.initDefaultConfig();
    ca::Initializer::SetNodeIp(0, 0, "172.26.160.193", "");
    ca::Initializer::SetNodeIp(0, 1, "172.26.160.194", "");
    ca::Initializer::SetNodeIp(0, 2, "172.26.160.195", "");

    auto d = ca::Dispatcher("/home/user", "nc_bft", "ncp");
    d.overrideProperties();

    std::vector<std::string> ips = {"47.92.107.184", "47.92.206.117", "47.92.193.119", "47.92.97.14"};
    if (!d.transmitFileParallel(ips, false, true)) {
        return -1;
    }
    for (int i=0; i<3; i++) {
        ca::Initializer::SetLocalId(0, i);
        if (!d.transmitPropertiesToRemote(ips[i])) {
            return -1;
        }
    }

    ca::Initializer::SetLocalId(0, 0);
    if (!d.transmitPropertiesToRemote(ips[3])) {
        return -1;
    }
    if (!d.generateDatabaseParallel(ips, "ycsb")) {
        return -1;
    };
    auto chan0 = d.startPeerParallel({ips[0], ips[1], ips[2]});
    sleep(20);
    auto chan3 = d.startUser(ips[3]);
    chan3->waitUntilCommandFinished(true);
    return 0;
}