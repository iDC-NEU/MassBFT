//
// Created by user on 23-5-15.
//

#include "common/zmq_port_util.h"

namespace util {

    class PortCalculator {
    public:
        PortCalculator(std::vector<int> regionsNodeCount, int groupId, int nodeId, int offset)
                : _regionsNodeCount(std::move(regionsNodeCount)), _portOffset(offset), _groupId(groupId), _nodeId(nodeId) {
            const int regionCount = (int)_regionsNodeCount.size();
            if (regionCount <= _groupId || _regionsNodeCount.at(_groupId) <= _nodeId) {
                LOG(ERROR) << "node index out of range!";
                CHECK(false);
            }
        }

        // It is used when a machine needs to monitor a port,
        // vector represents the ports monitored by other nodes in the same group,
        // and size is the number of nodes in the current group
        int newHeterogeneousLocalServicePorts() {
            int retCode = -1;
            for (int i = 0; i < (int) _regionsNodeCount.size(); i++) {
                auto totalNodes = _regionsNodeCount.at(i);
                if (i != _groupId) {
                    // skip regions
                    _portOffset += totalNodes;
                    continue;
                }
                std::vector<int> result(totalNodes);
                for (int j = 0; j < totalNodes; j++) {
                    // Iterate through nodes in local region
                    result[j] = _portOffset++;
                }
                if (!isPortValid()) {
                    return retCode;
                }
                retCode = (int)_results.size();
                _results.push_back(std::move(result));
            }
            return retCode;
        }

        // A machine needs to listen to multiple ports to receive (or send) messages from multiple groups,
        // and size is the number of groups
        int newHeterogeneousRemoteServicePorts() {
            int retCode = -1;
            auto tmpOffset = _portOffset;
            auto groupCount = (int)_regionsNodeCount.size();
            // calculate real _portOffset
            for (int i=0; i<groupCount; i++) {
                _portOffset += groupCount*_regionsNodeCount.at(i);
            }
            // use tmpOffset to skip regions
            for (int i=0; i<_groupId; i++) {
                tmpOffset += groupCount*_regionsNodeCount.at(i);
            }
            // skip nodes with smaller id (in local region)
            tmpOffset += groupCount*_nodeId;
            std::vector<int> result(groupCount);
            for (int i=0; i<groupCount; i++) {
                // as server, receive from multiple regions
                result[i] = tmpOffset;
                tmpOffset++;
            }
            if (!isPortValid()) {
                return retCode;
            }
            retCode = (int)_results.size();
            _results.push_back(std::move(result));
            return retCode;
        }

        int newHomogeneousLocalServicePorts() {
            int retCode = -1;
            auto totalNodes = _regionsNodeCount.at(_groupId);
            std::vector<int> result(totalNodes, _portOffset++);
            if (!isPortValid()) {
                return retCode;
            }
            retCode = (int)_results.size();
            _results.push_back(std::move(result));
            return retCode;
        }

        int newHomogeneousRemoteServicePorts() {
            int retCode = -1;
            auto groupCount = (int)_regionsNodeCount.size();
            std::vector<int> result(groupCount);
            for (int i=0; i<groupCount; i++) {
                result[i] = _portOffset++;
            }
            if (!isPortValid()) {
                return retCode;
            }
            retCode = (int)_results.size();
            _results.push_back(std::move(result));
            return retCode;
        }

        [[nodiscard]] inline const std::vector<int>& getPorts(int index) const {
            DCHECK((int)_results.size() > index && index >= 0) << "index out of range.";
            return _results.at(index);
        }

    protected:
        [[nodiscard]] inline bool isPortValid() const {
            if (_portOffset > 65535 || _portOffset <= 0) {
                LOG(INFO) << "port index out of range!";
                return false;
            }
            return true;
        }

    private:
        const std::vector<int> _regionsNodeCount;
        int _portOffset;
        const int _groupId;
        const int _nodeId;
        std::vector<std::vector<int>> _results;
    };

    std::shared_ptr<std::unordered_map<int, ZMQPortUtilList>> ZMQPortUtil::InitPortsConfig(int portOffset, const std::unordered_map<int, int> &regionNodesCount, bool distributed) {
        // init frServerPorts and rfrServerPorts
        std::unordered_map<int, ZMQPortUtilList> zmqPortsConfig;
        // default config(i.e. run in the same host machine)
        for (const auto& it: regionNodesCount) {   // it: pair<regionId, node count>
            zmqPortsConfig[it.first].resize(it.second);
            for (int i=0; i<it.second; i++) {   // zmqPortsConfig of a node
                zmqPortsConfig[it.first][i] = ZMQPortUtil::NewZMQPortUtil(regionNodesCount, it.first, i, portOffset, distributed);
            }
        }
        return std::make_shared<decltype(zmqPortsConfig)>(std::move(zmqPortsConfig));
    }

    int ZMQPortUtil::getRemoteServicePort(PortType portType, int regionId) const {
        auto it = _remoteServicePortMapper.find(portType);
        if (it == _remoteServicePortMapper.end()) {
            LOG(ERROR) << "can not find specific key: " << (int)it->first;
            return {};
        }
        auto ret = _portCalculator->getPorts(it->second);
        return ret.at(regionId);
    }

    std::unordered_map<int, int> ZMQPortUtil::getRemoteServicePorts(PortType portType) const {
        auto it = _remoteServicePortMapper.find(portType);
        if (it == _remoteServicePortMapper.end()) {
            LOG(ERROR) << "can not find specific key: " << (int)it->first;
            return {};
        }
        auto ret = _portCalculator->getPorts(it->second);
        std::unordered_map<int, int> remoteServicePorts;
        for (int i=0; i<(int)ret.size(); i++) {
            remoteServicePorts[i] = ret.at(i);
        }
        return remoteServicePorts;
    }

    const std::vector<int> &ZMQPortUtil::getLocalServicePorts(PortType portType, int groupId) const {
        auto it = _localServicePortMapper.find(portType);
        if (it == _localServicePortMapper.end()) {
            CHECK(false) << "can not find specific key: " << (int)it->first;
        }
        if ((int)it->second.size() <= groupId) {
            CHECK(false) << "can not find specific groupId: " << groupId;
        }
        return _portCalculator->getPorts(it->second.at(groupId));
    }

    void ZMQPortUtil::initWithDistributedDeployment() {
        _localServicePortMapper[PortType::SERVER_TO_SERVER].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::CLIENT_TO_SERVER].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::USER_REQ_COLLECTOR].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::BFT_PAYLOAD].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::BFT_RPC].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::CFT_PEER_TO_PEER].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _localServicePortMapper[PortType::LOCAL_BLOCK_ORDER].push_back(_portCalculator->newHomogeneousLocalServicePorts());
        _remoteServicePortMapper[PortType::LOCAL_FRAGMENT_BROADCAST] = _portCalculator->newHomogeneousRemoteServicePorts();
        _remoteServicePortMapper[PortType::REMOTE_FRAGMENT_RECEIVE] = _portCalculator->newHomogeneousRemoteServicePorts();
    }

    void ZMQPortUtil::initWithLocalDeployment() {
        _localServicePortMapper[PortType::SERVER_TO_SERVER].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::CLIENT_TO_SERVER].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::USER_REQ_COLLECTOR].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::BFT_PAYLOAD].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::BFT_RPC].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::CFT_PEER_TO_PEER].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _localServicePortMapper[PortType::LOCAL_BLOCK_ORDER].push_back(_portCalculator->newHeterogeneousLocalServicePorts());
        _remoteServicePortMapper[PortType::LOCAL_FRAGMENT_BROADCAST] = _portCalculator->newHeterogeneousRemoteServicePorts();
        _remoteServicePortMapper[PortType::REMOTE_FRAGMENT_RECEIVE] = _portCalculator->newHeterogeneousRemoteServicePorts();
    }

    std::unique_ptr<ZMQPortUtil> ZMQPortUtil::NewZMQPortUtil(const std::unordered_map<int, int> &regionNodesCount, int groupId, int nodeId, int offset, bool distributed) {
        std::vector<int> nodeCountVector;
        for (const auto& it: regionNodesCount) {
            if ((int)nodeCountVector.size() <= it.first) {
                nodeCountVector.resize(it.first + 1);
            }
            nodeCountVector[it.first] = it.second;
        }
        return NewZMQPortUtil(std::move(nodeCountVector), groupId, nodeId, offset, distributed);
    }

    std::unique_ptr<ZMQPortUtil> ZMQPortUtil::NewZMQPortUtil(std::vector<int> regionNodesCount, int groupId, int nodeId, int offset, bool distributed) {
        std::unique_ptr<ZMQPortUtil> zmqPortUtil(new ZMQPortUtil);
        zmqPortUtil->_portCalculator = std::make_unique<PortCalculator>(std::move(regionNodesCount), groupId, nodeId, offset);
        // init ports
        if (distributed) {
            zmqPortUtil->initWithDistributedDeployment();
        } else {
            zmqPortUtil->initWithLocalDeployment();
        }
        return zmqPortUtil;
    }

    std::shared_ptr<std::unordered_map<int, ZMQPortUtilList>> ZMQPortUtil::InitPortsConfig(const Properties &properties) {
        auto np = properties.getNodeProperties();
        std::unordered_map<int, int> regionNodesCount;
        for (int i=0; i<np.getGroupCount(); i++) {
            regionNodesCount[i] = np.getGroupNodeCount(i);
        }
        return InitPortsConfig(properties.replicatorLowestPort(), regionNodesCount, properties.isDistributedSetting());
    }

    std::unique_ptr<ZMQPortUtil> ZMQPortUtil::InitLocalPortsConfig(const Properties &properties) {
        auto np = properties.getNodeProperties();
        std::unordered_map<int, int> regionNodesCount;
        for (int i=0; i<np.getGroupCount(); i++) {
            regionNodesCount[i] = np.getGroupNodeCount(i);
        }
        auto groupId = np.getLocalNodeInfo()->groupId;
        auto nodeId = np.getLocalNodeInfo()->nodeId;
        return ZMQPortUtil::NewZMQPortUtil(regionNodesCount, groupId, nodeId, properties.replicatorLowestPort(), properties.isDistributedSetting());
    }

    ZMQPortUtil::~ZMQPortUtil() = default;
}
