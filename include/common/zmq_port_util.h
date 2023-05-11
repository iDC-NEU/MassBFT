//
// Created by user on 23-3-20.
//

#pragma once

#include "common/property.h"
#include <unordered_map>
#include <vector>
#include "glog/logging.h"

namespace util {
    // Deterministic generate zmq ports
    class ZMQPortUtil {
    protected:
        ZMQPortUtil() = default;

        static auto InitDistributedPortsConfig(int portOffset, const std::unordered_map<int, int>& regionNodesCount);

        static auto InitSingleHostPortsConfig(int portOffset, const std::unordered_map<int, int>& regionNodesCount);

    public:
        // samePort: each node using the same port for a service, for distributed tested
        static auto InitPortsConfig(int portOffset, const std::unordered_map<int, int>& regionNodesCount, bool samePort);

        virtual ~ZMQPortUtil() = default;

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

        // TODO: SUPPORT MULTI BFT
        [[nodiscard]] inline const auto& getServerToServerPorts() const { return serverToServerPorts; }

        // TODO: SUPPORT MULTI BFT
        [[nodiscard]] inline const auto& getClientToServerPorts() const { return clientToServerPorts; }

        [[nodiscard]] inline const auto& getRequestCollectorPorts() const { return userRequestCollectorPorts; }

        [[nodiscard]] inline const auto& getBFTPayloadSeparationPorts() const { return bftPayloadSeparationPorts; }

        // TODO: SUPPORT MULTI BFT
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

    protected:
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

    using ZMQPortUtilList = std::vector<std::unique_ptr<ZMQPortUtil>>;

    class SingleServerZMQPortUtil : public ZMQPortUtil {
    public:
        // offset: the next port unused.
        SingleServerZMQPortUtil(const std::unordered_map<int, int>& regionsNodeCount, int regionId, int nodeId, int offset)
                : SingleServerZMQPortUtil([&]{
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

        SingleServerZMQPortUtil(const std::vector<int>& regionsNodeCount, int regionId, int nodeId, int offset) {
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
            // Allocate rfr ports and FR ports
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
    };

    class DistributedZMQPortUtil : public ZMQPortUtil {
    public:
        // offset: the next port unused.
        DistributedZMQPortUtil(const std::unordered_map<int, int>& regionsNodeCount, int regionId, int offset)
                : DistributedZMQPortUtil([&]{
            // covert into vector
            std::vector<int> ret;
            for (const auto& it: regionsNodeCount) {
                if ((int)ret.size() <= it.first) {
                    ret.resize(it.first + 1);
                }
                ret[it.first] = it.second;
            }
            return ret;
        }(), regionId, offset) { }

        DistributedZMQPortUtil(const std::vector<int>& regionsNodeCount, int regionId, int offset) {
            const int regionCount = (int)regionsNodeCount.size();
            if (regionCount <= regionId) {
                LOG(ERROR) << "node index out of range!";
                CHECK(false);
            }
            auto totalRegions = (int)regionsNodeCount.size();
            auto tmpOffset = offset;
            serverToServerPorts = std::vector<int>(totalRegions, tmpOffset++);
            clientToServerPorts = std::vector<int>(totalRegions, tmpOffset++);
            userRequestCollectorPorts = std::vector<int>(totalRegions, tmpOffset++);
            bftPayloadSeparationPorts = std::vector<int>(totalRegions, tmpOffset++);
            bftRpcPorts = std::vector<int>(totalRegions, tmpOffset++);
            for (int i=0; i<totalRegions; i++) {
                frPorts.push_back(tmpOffset++);
                rfrPorts.push_back(tmpOffset++);
            }
        }

        void printConfig() const {
            LOG(INFO) << "Config ports of distributed setting:";
            LOG(INFO) << "serverToServerPorts: " << serverToServerPorts[0];
            LOG(INFO) << "clientToServerPorts: " << clientToServerPorts[0];
            LOG(INFO) << "userRequestCollectorPorts: " << userRequestCollectorPorts[0];
            LOG(INFO) << "bftPayloadSeparationPorts: " << bftPayloadSeparationPorts[0];
            LOG(INFO) << "bftRpcPorts: " << bftRpcPorts[0];
            LOG(INFO) << "frPorts: " << frPorts[0];
            LOG(INFO) << "rfrPorts: " << rfrPorts[0];
        }
    };


    inline auto ZMQPortUtil::InitDistributedPortsConfig(int portOffset, const std::unordered_map<int, int>& regionNodesCount)  {
        // ---- print debug info ----
        auto demoConfig = std::make_unique<DistributedZMQPortUtil>(regionNodesCount, 0, portOffset);
        demoConfig->printConfig();
        // init frServerPorts and rfrServerPorts
        std::unordered_map<int, ZMQPortUtilList> zmqPortsConfig;
        for (const auto& it: regionNodesCount) {
            for (int i=0; i<it.second; i++) {
                auto portsConfig = std::make_unique<DistributedZMQPortUtil>(regionNodesCount, it.first, portOffset);
                zmqPortsConfig[it.first].push_back(std::move(portsConfig));
            }
        }
        return zmqPortsConfig;
    }

    inline auto ZMQPortUtil::InitSingleHostPortsConfig(int portOffset, const std::unordered_map<int, int> &regionNodesCount){
        // init frServerPorts and rfrServerPorts
        std::unordered_map<int, ZMQPortUtilList> zmqPortsConfig;
        // default config(i.e. run in the same host machine)
        for (const auto& it: regionNodesCount) {   // it: pair<regionId, node count>
            zmqPortsConfig[it.first].resize(it.second);
            for (int i=0; i<it.second; i++) {   // zmqPortsConfig of a node
                zmqPortsConfig[it.first][i] = std::make_unique<SingleServerZMQPortUtil>(
                        regionNodesCount,
                        it.first,   // region id
                        i,          // node id
                        portOffset);
            }
        }
        return zmqPortsConfig;
    }

    // return a shared pointer of port map
    inline auto ZMQPortUtil::InitPortsConfig(int portOffset, const std::unordered_map<int, int> &regionNodesCount, bool useSamePort) {
        if (!useSamePort) {
            auto ret = InitSingleHostPortsConfig(portOffset, regionNodesCount);
            return std::make_shared<decltype(ret)>(std::move(ret));
        }
        auto ret = InitDistributedPortsConfig(portOffset, regionNodesCount);
        return std::make_shared<decltype(ret)>(std::move(ret));
    }
}