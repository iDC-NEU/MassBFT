//
// Created by peng on 10/18/22.
//

#pragma  once

#include <mutex>
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"

namespace util {

    struct NodeConfig {
        int nodeId = -1;
        int groupId = -1;
        std::string ski;
        std::string priIp;
        std::string pubIp;

        bool operator==(const NodeConfig& rhs) const {
            if (this->nodeId != rhs.nodeId) {
                return false;
            }
            if (this->groupId != rhs.groupId) {
                return false;
            }
            DCHECK(this->ski == rhs.ski);
            DCHECK(this->priIp == rhs.priIp);
            DCHECK(this->pubIp == rhs.pubIp);
            return true;
        }
    };
    using NodeConfigPtr = std::shared_ptr<NodeConfig>;

    struct ZMQInstanceConfig {
        util::NodeConfigPtr nodeConfig;
        int port;

        [[nodiscard]] std::string& priAddr() const {
            DCHECK(nodeConfig != nullptr) << "nodeConfig unset!";
            return nodeConfig->priIp;
        }

        [[nodiscard]] std::string& pubAddr() const {
            DCHECK(nodeConfig != nullptr) << "nodeConfig unset!";
            return nodeConfig->pubIp;
        }

        bool operator==(const ZMQInstanceConfig& rhs) const {
            if (this->port != rhs.port) {
                return false;
            }
            return *this->nodeConfig == *rhs.nodeConfig;
        }
    };

    class ChaincodeProperties {
    public:
        ChaincodeProperties(const ChaincodeProperties& rhs) = default;

        ChaincodeProperties(ChaincodeProperties&& rhs) noexcept : n(rhs.n) { }

        constexpr static const auto INIT_ON_STARTUP = "init";

    public:
        bool installed(const std::string& ccName) const {
            // exist and not null
            return n[ccName].IsDefined() && !n[ccName].IsNull();
        }

        std::vector<std::string> installed() const {
            std::vector<std::string> installedCC;
            for (auto& it: n) {
                auto ret = it.first.as<std::string>("");
                if (!ret.empty()) {
                    installedCC.push_back(ret);
                }
            }
            return installedCC;
        }

        void install(const std::string& ccName) {
            n[ccName] = NewCCNode();
        }

    protected:
        friend class Properties;

        explicit ChaincodeProperties(const YAML::Node& node) :n(node) { }

        static YAML::Node NewCCNode(bool initOnStartup=true) {
            YAML::Node ret;
            ret[INIT_ON_STARTUP] = initOnStartup;
            return ret;
        };

    private:
        YAML::Node n;
    };

    class NodeProperties {
    public:
        NodeProperties(const NodeProperties& rhs) = default;

        NodeProperties(NodeProperties&& rhs) noexcept : n(rhs.n) { }

        constexpr static const auto INIT_ON_STARTUP = "init";

    protected:
        static auto BuildGroupKey(int groupId) { return std::string("group_") + std::to_string(groupId); }

        std::pair<YAML::Node, bool> getAndCheckGroup(int groupId) const {
            auto groupKey = BuildGroupKey(groupId);
            auto list = n[groupKey];
            if (!list.IsDefined() || list.IsNull() || !list.IsSequence()) {
                return std::make_pair(YAML::Node{}, false);
            }
            return std::make_pair(list, true);
        }

        static NodeConfigPtr LoadNodeInfo(const YAML::Node& node) {
            try {
                NodeConfigPtr cfg(new NodeConfig);
                cfg->nodeId = node["node_id"].as<int>();
                cfg->groupId = node["group_id"].as<int>();
                cfg->ski = node["ski"].as<std::string>();
                cfg->priIp = node["pri_ip"].as<std::string>(); // load private ip first (public ip may not exist)
                cfg->pubIp = node["pub_ip"].as<std::string>();
                return cfg;
            } catch (const YAML::Exception &e) {
                LOG(WARNING) << "Can not load node config: " << e.what();
            }
            return nullptr;
        }

    public:
        // NodeConfigPtr may be empty
        NodeConfigPtr getSingleNodeInfo(int groupId, int nodeId) const {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return nullptr; // group not found
            }
            if ((int)list.size() <= nodeId) {
                return nullptr; // node not found
            }
            return LoadNodeInfo(list[nodeId]);
        }

        std::vector<NodeConfigPtr> getGroupNodesInfo(int groupId) const {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return {}; // group not found
            }
            std::vector<NodeConfigPtr> nodeList;
            for (const auto& it: list) {
                auto ret = LoadNodeInfo(it);
                if (ret == nullptr) {
                    continue;
                }
                nodeList.push_back(ret);
            }
            return nodeList;
        }

        NodeConfigPtr getLocalNodeInfo() const {
            try {
                auto groupId = n["local_group_id"].as<int>();
                auto nodeId = n["local_node_id"].as<int>();
                return getSingleNodeInfo(groupId, nodeId);
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not get local node id: " << e.what();
            }
            return nullptr;
        }

        int getGroupCount() const {
            if (!n.IsDefined() || !n.IsMap()) {
                return -1;
            }
            for (int i=0;; i++) {
                auto [list, success] = getAndCheckGroup(i);
                if (!success) {
                    return i;
                }
            }
        }

        int getGroupNodeCount(int groupId) const {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return -1; // group not found
            }
            return (int)list.size();
        }

        void setSingleNodeInfo(const NodeConfigPtr& cfg) {
            YAML::Node node;
            node["node_id"] = cfg->nodeId;
            node["group_id"] = cfg->groupId;
            node["ski"] = cfg->ski;
            node["pri_ip"] = cfg->priIp;
            node["pub_ip"] = cfg->pubIp;
            auto groupKey = BuildGroupKey(cfg->groupId);
            auto list = n[groupKey];
            list.push_back(node);
        }

        void setLocalNodeInfo(int groupId, int nodeId) {
            n["local_group_id"] = groupId;
            n["local_node_id"] = nodeId;
        }

    protected:
        friend class Properties;

        explicit NodeProperties(const YAML::Node& node) :n(node) { }

    private:
        YAML::Node n;
    };

    class Properties {
    private:
        static inline std::shared_ptr<Properties> properties;

    public:
        constexpr static const auto CHAINCODE_PROPERTIES = "chaincode";
        constexpr static const auto NODES_PROPERTIES = "nodes";
        constexpr static const auto START_BLOCK_NUMBER = "start_at";
        constexpr static const auto DISTRIBUTED_SETTING = "distributed";
        constexpr static const auto REPLICATOR_LOWEST_PORT = "replicator_lowest_port";
        constexpr static const auto SSH_USERNAME = "ssh_username";
        constexpr static const auto SSH_PASSWORD = "ssh_password";
        constexpr static const auto JVM_PATH = "jvm_path";
        constexpr static const auto BATCH_TIMEOUT_MS = "batch_timeout_ms";
        constexpr static const auto BATCH_MAX_SIZE = "batch_max_size";
        constexpr static const auto VALIDATE_USER_REQUEST_ON_RECEIVE = "validate_on_receive";

    public:
        // Load from file, if fileName is null, create an empty property
        static bool LoadProperties(const std::string& fileName = {}) {
            if (fileName.empty()) {
                properties = std::make_unique<Properties>();
                YAML::Node node;
                node[CHAINCODE_PROPERTIES] = {};
                node[NODES_PROPERTIES] = {};
                properties->_node = node;
                return true;
            }
            properties = std::make_unique<Properties>();
            try {
                auto node = YAML::LoadFile(fileName);
                if (!node[CHAINCODE_PROPERTIES].IsDefined() || node[CHAINCODE_PROPERTIES].IsNull()) {
                    LOG(ERROR) << "CHAINCODE_PROPERTIES not exist.";
                    return false;
                }
                if (!node[NODES_PROPERTIES].IsDefined() || node[NODES_PROPERTIES].IsNull()) {
                    LOG(ERROR) << "NODES_PROPERTIES not exist.";
                    return false;
                }
                properties->_node = node;
                return true;
            } catch (const YAML::Exception &e) {
                LOG(ERROR) << "Can not load config: " << e.what();
            }
            return false;
        }

        static Properties *GetProperties() {
            DCHECK(properties != nullptr) << "properties is not generated (or loaded) yet";
            return properties.get();
        }

        static std::shared_ptr<Properties> GetSharedProperties() {
            DCHECK(properties != nullptr);
            return properties;
        }

        static void SetProperties(auto&& key, auto&& value) {
            auto* p = util::Properties::GetProperties();
            p->_node[key] = value;
        }

        ~Properties() = default;

        ChaincodeProperties getChaincodeProperties() const { return ChaincodeProperties(_node[CHAINCODE_PROPERTIES]); }

        NodeProperties getNodeProperties() const { return NodeProperties(_node[NODES_PROPERTIES]); }

        YAML::Node getCustomProperties(const std::string& key) {
            if (!_node[key].IsDefined()) {
                _node[key].reset();
            }
            return _node[key];
        }

        YAML::Node getCustomPropertiesOrPanic(const std::string& key) const {
            if (!_node[key].IsDefined()) {
                CHECK(false) << "Can not find key: " << key;
            }
            return _node[key];
        }

        int getStartBlockNumber(int groupId) const {
            int blockNumber = 0;
            try {
                blockNumber = _node[START_BLOCK_NUMBER][groupId].as<int>(0);
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find START_BLOCK_NUMBER for group " << groupId << ", start from 0.";
            }
            return blockNumber;
        }

        bool isDistributedSetting() const {
            bool dist = false;
            try {
                dist = _node[DISTRIBUTED_SETTING].as<bool>(false);
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find DISTRIBUTED_SETTING key, fallback to false.";
            }
            return dist;
        }

        int replicatorLowestPort() const {
            int port = 51200;
            try {
                port = _node[REPLICATOR_LOWEST_PORT].as<int>();
                if (port<=0 || port > 65535) {
                    port = 51200;
                }
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find REPLICATOR_LOWEST_PORT key, fallback to 51200.";
            }
            return port;
        }

        std::tuple<std::string, std::string, bool> getSSHInfo() const {
            try {
                return { _node[SSH_USERNAME].as<std::string>(),
                         _node[SSH_PASSWORD].as<std::string>(),
                         true };
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not load SSH info, fallback to default.";
            }
            return {"user", "123456", false};
        }

        std::string getJVMPath() const {
            try {
                return _node[JVM_PATH].as<std::string>();
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find JVM_PATH, leave it to empty.";
            }
            return {};
        }

        int getBlockBatchTimeoutMs() const {
            try {
                return _node[BATCH_TIMEOUT_MS].as<int>();
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find BATCH_TIMEOUT_MS, leave it to 100.";
            }
            return 100; // 100ms
        }

        int getBlockMaxBatchSize() const {
            try {
                return _node[BATCH_MAX_SIZE].as<int>();
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find BATCH_MAX_SIZE, leave it to 200.";
            }
            return 200; // 200 size
        }

        // validate user request immediately, instead of validate them during consensus
        bool validateOnReceive() const {
            bool dist = false;
            try {
                dist = _node[VALIDATE_USER_REQUEST_ON_RECEIVE].as<bool>(false);
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not find VALIDATE_USER_REQUEST_ON_RECEIVE key, fallback to false.";
            }
            return dist;
        }

    private:
        YAML::Node _node;
    };
}

