//
// Created by user on 23-3-20.
//

#pragma once

#include "common/property.h"
#include <unordered_map>
#include <vector>
#include "glog/logging.h"

namespace peer::v2 {
    // Deterministic generate zmq ports
    class ZMQPortUtil {
    public:
        // offset: the next port unused.
        ZMQPortUtil(const std::unordered_map<int, int>& regionsNodeCount, int regionId, int nodeId, int offset)
         : ZMQPortUtil([&]{
            // covert into vector
            std::vector<int> ret;
            for (const auto& it: regionsNodeCount) {
                if ((int)ret.size() <= it.first) {
                    ret.resize(it.first + 1);
                }
                ret[it.first] = it.second;
            }
            return ret;
         }(), regionId, nodeId, offset) { }

        ZMQPortUtil(const std::vector<int>& regionsNodeCount, int regionId, int nodeId, int offset) {
            const int regionCount = (int)regionsNodeCount.size();
            if (regionCount <= regionId || regionsNodeCount.at(regionId) <= nodeId) {
                LOG(ERROR) << "node index out of range!";
                CHECK(false);
            }
            int realPortOffset = offset;
            // Allocate pbft ports
            for (int i=0; i<(int)regionsNodeCount.size(); i++) {
                auto totalNodes = regionsNodeCount.at(i);
                if (i == regionId) {
                    serverToServerPorts.resize(totalNodes);
                    clientToServerPorts.resize(totalNodes);
                    userRequestCollectorPorts.resize(totalNodes);
                    bftPayloadSeparationPorts.resize(totalNodes);
                    bftRpcPorts.resize(totalNodes);
                    for (int j=0; j<totalNodes; j++) {
                        // Iterate through nodes in local region
                        serverToServerPorts[j] = realPortOffset++;
                        clientToServerPorts[j] = realPortOffset++;
                        userRequestCollectorPorts[j] = realPortOffset++;
                        bftPayloadSeparationPorts[j] = realPortOffset++;
                        bftRpcPorts[j] = realPortOffset++;
                    }
                    continue;
                }
                realPortOffset += 5*totalNodes;
            }
            // Allocate rfr ports and fr ports
            for (int i=0; i<regionId; i++) {
                realPortOffset += regionCount*2*regionsNodeCount.at(i);
            }
            // skip nodes with smaller id (in local region)
            realPortOffset += regionCount*2*nodeId;
            frPorts.resize(regionCount);
            for (int i=0; i<regionCount; i++) {
                frPorts[i] = realPortOffset;
                realPortOffset++;
            }
            rfrPorts.resize(regionCount);
            for (int i=0; i<regionCount; i++) {
                rfrPorts[i] = realPortOffset;
                realPortOffset++;
            }
        }

        [[nodiscard]] inline int getFRServerPort(int regionId) const {
            return frPorts.at(regionId);
        }

        [[nodiscard]] inline int getRFRServerPort(int regionId) const {
            return rfrPorts.at(regionId);
        }

        [[nodiscard]] inline std::unordered_map<int, int> getFRServerPorts() const {
            std::unordered_map<int, int> ret;
            for (int i=0; i<(int)frPorts.size(); i++) {
                ret[i] = frPorts.at(i);
            }
            return ret;
        }

        [[nodiscard]] inline std::unordered_map<int, int> getRFRServerPorts() const {
            std::unordered_map<int, int> ret;
            for (int i=0; i<(int)rfrPorts.size(); i++) {
                ret[i] = rfrPorts.at(i);
            }
            return ret;
        }

        [[nodiscard]] inline const auto& getServerToServerPorts() const { return serverToServerPorts; }

        [[nodiscard]] inline const auto& getClientToServerPorts() const { return clientToServerPorts; }

        [[nodiscard]] inline const auto& getRequestCollectorPorts() const { return userRequestCollectorPorts; }

        [[nodiscard]] inline const auto& getBFTPayloadSeparationPorts() const { return bftPayloadSeparationPorts; }

        [[nodiscard]] inline const auto& getBFTRpcPorts() const { return bftRpcPorts; }

        static bool WrapPortWithConfig(const std::vector<std::shared_ptr<util::NodeConfig>>& nodes,
                                       const std::vector<int>& ports,
                                       std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& zmqInstances) {
            if (nodes.empty() || nodes.size() != ports.size()) {
                return false;
            }
            zmqInstances.clear();
            for (int i=0; i<(int)nodes.size(); i++) {
                auto zmqCfg = std::make_unique<util::ZMQInstanceConfig>();
                zmqCfg->nodeConfig = nodes[i];
                zmqCfg->port = ports[i];
                zmqInstances.push_back(std::move(zmqCfg));
            }
            return true;
        }

    private:
        // PBFT server to server ports in local region
        std::vector<int> serverToServerPorts;
        // PBFT client to server ports in local region
        std::vector<int> clientToServerPorts;
        // Ports used for local region user collector
        std::vector<int> userRequestCollectorPorts;
        // The port used to separate the pbft consensus message from the real content
        std::vector<int> bftPayloadSeparationPorts;
        // The port used to connect the peer with the bft instance
        std::vector<int> bftRpcPorts;
        // broadcast in the local zone, key region id, value port (as ZMQServer)
        std::vector<int> frPorts;
        // receive from crossRegionSender, key region id, value port (as ReliableZmqServer)
        std::vector<int> rfrPorts;
    };
}