//
// Created by user on 23-3-20.
//

#pragma once

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

    private:
        // broadcast in the local zone, key region id, value port (as ZMQServer)
        std::vector<int> frPorts;
        // receive from crossRegionSender, key region id, value port (as ReliableZmqServer)
        std::vector<int> rfrPorts;
    };
}