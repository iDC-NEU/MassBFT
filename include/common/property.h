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
        std::string ip;

        bool operator==(const NodeConfig& rhs) const {
            if (this->nodeId != rhs.nodeId) {
                return false;
            }
            if (this->groupId != rhs.groupId) {
                return false;
            }
            DCHECK(this->ski == rhs.ski);
            DCHECK(this->ip == rhs.ip);
            return true;
        }
    };
    using NodeConfigPtr = std::shared_ptr<NodeConfig>;

    struct ZMQInstanceConfig {
        util::NodeConfigPtr nodeConfig;
        int port;

        [[nodiscard]] std::string& addr() const {
            DCHECK(nodeConfig != nullptr) << "nodeConfig unset!";
            return nodeConfig->ip;
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

    class YCSBProperties {
    public:
        YCSBProperties(const YCSBProperties& rhs) = default;

        YCSBProperties(YCSBProperties&& rhs) noexcept : n(rhs.n) { }

    public:

    protected:
        friend class Properties;

        explicit YCSBProperties(const YAML::Node& node) :n(node) { }

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

        static NodeConfigPtr LoadNodeInfo(const YAML::Node& node, bool publicAddress) {
            try {
                NodeConfigPtr cfg(new NodeConfig);
                cfg->nodeId = node["node_id"].as<int>();
                cfg->groupId = node["group_id"].as<int>();
                cfg->ski = node["ski"].as<std::string>();
                cfg->ip = node["pri_ip"].as<std::string>(); // load private ip first (public ip may not exist)
                if (publicAddress) {
                    if (!node["pub_ip"].IsDefined() || node["pub_ip"].IsNull()) {
                        LOG(WARNING) << "node: " << cfg->ski << " public ip not exist!";
                        return cfg;
                    }
                    cfg->ip = node["pub_ip"].as<std::string>();
                }
                return cfg;
            }
            catch (const YAML::Exception &e) {
                LOG(WARNING) << "Can not load node config: " << e.what();
            }
            return nullptr;
        }

        std::pair<YAML::Node, bool> getAndCheckGroup(int groupId) const {
            auto groupKey = BuildGroupKey(groupId);
            auto list = n[groupKey];
            if (!list.IsDefined() || list.IsNull() || !list.IsSequence()) {
                return std::make_pair(YAML::Node{}, false);
            }
            return std::make_pair(list, true);
        }

    public:
        // NodeConfigPtr may be empty
        NodeConfigPtr getSingleNodeInfo(int groupId, int nodeId, bool publicAddress) const {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return nullptr; // group not found
            }
            if ((int)list.size() <= nodeId) {
                return nullptr; // node not found
            }
            return LoadNodeInfo(list[nodeId], publicAddress);
        }

        std::vector<NodeConfigPtr> getGroupNodesInfo(int groupId, bool publicAddress) const {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return {}; // group not found
            }
            std::vector<NodeConfigPtr> nodeList;
            for (const auto& it: list) {
                auto ret = LoadNodeInfo(it, publicAddress);
                if (ret == nullptr) {
                    continue;
                }
                nodeList.push_back(ret);
            }
            return nodeList;
        }

        int getGroupCount(bool check=true) {
            if (!n.IsDefined() || !n.IsMap()) {
                return -1;
            }
            auto ret = (int)n.size();
            if (!check) {
                return ret;
            }
            for (int i=0; i<ret; i++) {
                auto [list, success] = getAndCheckGroup(i);
                if (!success) {
                    return -1; // group not found
                }
            }
            return ret;
        }

        int getGroupNodeCount(int groupId) {
            auto [list, success] = getAndCheckGroup(groupId);
            if (!success) {
                return -1; // group not found
            }
            return (int)list.size();
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

        constexpr static const auto YCSB_PROPERTIES = "ycsb";
        constexpr static const auto CHAINCODE_PROPERTIES = "chaincode";
        constexpr static const auto NODES_PROPERTIES = "nodes";

    public:
        // Load from file, if fileName is null, create an empty property
        static void LoadProperties(const std::string& fileName = {}) {
            if (fileName.empty()) {
                properties = std::make_unique<Properties>();
                YAML::Node node;
                node[YCSB_PROPERTIES] = {};
                node[CHAINCODE_PROPERTIES] = {};
                node[NODES_PROPERTIES] = {};
                properties->_node = node;
                return;
            }
            properties = std::make_unique<Properties>();
            try {
                auto node = YAML::LoadFile(fileName);
                CHECK(node[YCSB_PROPERTIES].IsDefined() && !node[YCSB_PROPERTIES].IsNull());
                CHECK(node[CHAINCODE_PROPERTIES].IsDefined() && !node[CHAINCODE_PROPERTIES].IsNull());
                CHECK(node[NODES_PROPERTIES].IsDefined() && !node[NODES_PROPERTIES].IsNull());
                properties->_node = node;
            }
            catch (const YAML::Exception &e) {
                CHECK(false) << "Can not load config: " << e.what();
            }
        }

        static Properties *GetProperties() {
            DCHECK(properties != nullptr);
            return properties.get();
        }

        static std::shared_ptr<Properties> GetSharedProperties() {
            DCHECK(properties != nullptr);
            return properties;
        }

        ~Properties() = default;

        YCSBProperties getYCSBProperties() const { return YCSBProperties(_node[YCSB_PROPERTIES]); }

        ChaincodeProperties getChaincodeProperties() const { return ChaincodeProperties(_node[CHAINCODE_PROPERTIES]); }

        NodeProperties getNodeProperties() const { return NodeProperties(_node[NODES_PROPERTIES]); }

        YAML::Node getCustomProperties(const std::string& key) {
            if (!_node[key].IsDefined()) {
                _node[key].reset();
            }
            return _node[key];
        }

        YAML::Node getCustomProperties(const std::string& key) const {
            if (!_node[key].IsDefined()) {
                CHECK(false) << "Can not find key: " << key;
            }
            return _node[key];
        }

    private:
        YAML::Node _node;
    };
}

