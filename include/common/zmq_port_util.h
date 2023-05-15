//
// Created by user on 23-3-20.
//

#pragma once

#include "common/property.h"
#include <unordered_map>
#include <vector>
#include "glog/logging.h"

namespace util {

    enum class PortType {
        // BFT server to server ports in local region
        SERVER_TO_SERVER = 0,
        // BFT client to server ports in local region
        CLIENT_TO_SERVER = 1,
        // Ports used for local region user collector
        USER_REQ_COLLECTOR = 2,
        // The port used to separate the pbft consensus message from the real content
        BFT_PAYLOAD = 3,
        // The port used to connect the peer with the bft instance
        BFT_RPC = 4,
        // broadcast in the local zone, key region id, value port (as ZMQServer)
        LOCAL_FRAGMENT_BROADCAST = 100,
        // receive from crossRegionSender, key region id, value port (as ReliableZmqServer)
        REMOTE_FRAGMENT_RECEIVE = 101,
    };

    class PortCalculator;
    class ZMQPortUtil;

    using ZMQPortUtilList = std::vector<std::unique_ptr<ZMQPortUtil>>;

    // Deterministic generate zmq ports
    class ZMQPortUtil {
    protected:
        ZMQPortUtil() = default;

        void initWithDistributedDeployment();

        void initWithLocalDeployment();

    public:
        static std::shared_ptr<std::unordered_map<int, ZMQPortUtilList>> InitPortsConfig(int portOffset, const std::unordered_map<int, int>& regionNodesCount, bool distributed);

        static std::unique_ptr<ZMQPortUtil> NewZMQPortUtil(const std::unordered_map<int, int> &regionNodesCount, int groupId, int nodeId, int offset, bool distributed);

        static std::unique_ptr<ZMQPortUtil> NewZMQPortUtil(std::vector<int> regionNodesCount, int groupId, int nodeId, int offset, bool distributed);

        ~ZMQPortUtil();

        [[nodiscard]] int getRemoteServicePort(PortType portType, int regionId) const;

        // key: region id, value, port id
        [[nodiscard]] std::unordered_map<int, int> getRemoteServicePorts(PortType portType) const;

        [[nodiscard]] const std::vector<int>& getLocalServicePorts(PortType portType, int groupId=0) const;

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

        void distributedPrintConfig() const {
            LOG(INFO) << "Config ports of distributed setting:";
            LOG(INFO) << "serverToServerPorts: " << getLocalServicePorts(PortType::SERVER_TO_SERVER)[0];   // node id
            LOG(INFO) << "clientToServerPorts: " << getLocalServicePorts(PortType::CLIENT_TO_SERVER)[0];
            LOG(INFO) << "userRequestCollectorPorts: " << getLocalServicePorts(PortType::USER_REQ_COLLECTOR)[0];
            LOG(INFO) << "bftPayloadSeparationPorts: " << getLocalServicePorts(PortType::BFT_PAYLOAD)[0];
            LOG(INFO) << "bftRpcPorts: " << getLocalServicePorts(PortType::BFT_RPC)[0];
            LOG(INFO) << "frPorts: " << getRemoteServicePorts(PortType::LOCAL_FRAGMENT_BROADCAST)[0];   // group id
            LOG(INFO) << "rfrPorts: " << getRemoteServicePorts(PortType::REMOTE_FRAGMENT_RECEIVE)[0];   // group id
        }

    private:
        std::unique_ptr<PortCalculator> _portCalculator;
        std::unordered_map<PortType, int> _remoteServicePortMapper;
        std::unordered_map<PortType, std::vector<int>> _localServicePortMapper;
    };

}