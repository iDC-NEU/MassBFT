//
// Created by user on 23-7-4.
//

#include "ca/config_initializer.h"
#include "common/ssh.h"
#include "glog/logging.h"

int main(int argc, char *argv[]) {
    ca::Initializer initializer({3, 3});
    initializer.initDefaultConfig();
    ca::Initializer::SetNodeIp(0, 0, "172.18.180.3", "");
    ca::Initializer::SetNodeIp(0, 1, "172.18.180.4", "");
    ca::Initializer::SetNodeIp(0, 2, "172.18.180.5", "");
    ca::Initializer::SetNodeIp(1, 0, "172.18.180.6", "");
    ca::Initializer::SetNodeIp(1, 1, "172.18.180.7", "");
    ca::Initializer::SetNodeIp(1, 2, "172.18.180.8", "");

    auto d = ca::Dispatcher("/home/user", "nc_bft", "ncp");
    d.overrideProperties();

    std::vector<std::string> ips = {"39.100.39.219", "39.100.35.198", "39.98.244.27", "39.100.35.19", "39.98.246.37", "39.100.40.119", "39.98.174.236", "39.100.36.227"};
    if (!d.processParallel(&ca::Dispatcher::transmitFileToRemote, ips)) {
        return -1;
    }
    if (!d.processParallel(&ca::Dispatcher::compileRemoteSourcecode, ips)) {
        return -1;
    }
    for (int i=0; i<3; i++) {
        ca::Initializer::SetLocalId(0, i);
        if (!d.transmitPropertiesToRemote(ips[i])) {
            return -1;
        }
    }
    for (int i=0; i<3; i++) {
        ca::Initializer::SetLocalId(1, i);
        if (!d.transmitPropertiesToRemote(ips[i+3])) {
            return -1;
        }
    }
    ca::Initializer::SetLocalId(0, 0);
    if (!d.transmitPropertiesToRemote(ips[6])) {
        return -1;
    }
    ca::Initializer::SetLocalId(1, 0);
    if (!d.transmitPropertiesToRemote(ips[7])) {
        return -1;
    }
    if (!d.processParallel(&ca::Dispatcher::generateDatabase, ips, "ycsb")) {
        return -1;
    }
    if (!d.processParallel(&ca::Dispatcher::backupRemoteDatabase, ips)) {
        return -1;
    }
    if (!d.processParallel(&ca::Dispatcher::stopAll, ips)) {
        return -1;
    }
    if (!d.processParallel(&ca::Dispatcher::restoreRemoteDatabase, ips)) {
        return -1;
    }
    std::vector<std::unique_ptr<util::SSHChannel>> sList(6);
    for (int i=0; i<6; i++) {
        sList[i] = d.startPeer(ips[i]);
    }
    std::this_thread::sleep_for(std::chrono::seconds(20));
    auto u1 = d.startUser(ips[6]);
    auto u2 = d.startUser(ips[7]);
    u1->waitUntilCommandFinished(true);
    u2->waitUntilCommandFinished(true);

    return 0;
}