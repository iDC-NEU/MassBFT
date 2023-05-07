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

    class Properties {
    private:
        static inline std::unique_ptr<Properties> p;
        static inline std::mutex mutex;
        constexpr static const auto YAML_CONFIG_FILE = "config.yaml";
        constexpr static const auto BK_YAML_CONFIG_FILE = "/tmp/config.yaml";

        constexpr static const auto YCSB_PROPERTY_KEY = "ycsb";
        constexpr static const auto NODES_INFO = "nodes";
        constexpr static const auto DEFAULT_CHAINCODE = "default_chaincode";

    public:
        // inside a lock (if called by GetProperties)
        static void InitProperties(const YAML::Node& node = {}) {
            p = std::make_unique<Properties>();
            p->n = node;
        }

        static Properties *GetProperties() {
            if (p == nullptr) {
                std::lock_guard lock(mutex);
                YAML::Node node;
                if (p == nullptr) {
                    try {
                        node = YAML::LoadFile(Properties::YAML_CONFIG_FILE);
                    }
                    catch (const YAML::Exception &e) {
                        LOG(ERROR) << Properties::YAML_CONFIG_FILE << " not exist, switch to bk file!";
                        try {
                            node = YAML::LoadFile(Properties::BK_YAML_CONFIG_FILE);
                        }
                        catch (const YAML::Exception &e) {
                            CHECK(false) << Properties::BK_YAML_CONFIG_FILE << " not exist!";
                        }
                    }
                    InitProperties(node);
                }
            }
            return p.get();
        }

        ~Properties() = default;

        YAML::Node getRaw() const {
            return n;
        }

        YAML::Node getYCSBProperties() const {
            return n[YCSB_PROPERTY_KEY];
        }

        auto getNodesInfo() const {
            std::vector<NodeConfigPtr> ret;
            for (const auto &it: n[NODES_INFO]) {
                NodeConfigPtr cfg(new NodeConfig);
                cfg->nodeId = it["node_id"].as<int>();
                cfg->groupId = it["group_id"].as<int>();
                cfg->ski = it["ski"].as<std::string>();
                cfg->ip = it["ski"].as<std::string>();
                ret.push_back(cfg);
            }
            return ret;
        }

        std::string getDefaultChaincodeName() const { return n[DEFAULT_CHAINCODE].as<std::string>(); }

        void setDefaultChaincodeName(const std::string& ccName) { n[DEFAULT_CHAINCODE] = ccName; }

    private:
        YAML::Node n;
    };
}

