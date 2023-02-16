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
    };

    class Properties {
    private:
        static inline std::unique_ptr<Properties> p;
        static inline std::mutex mutex;
        constexpr static const auto YAML_CONFIG_FILE = "config.yaml";
        constexpr static const auto BK_YAML_CONFIG_FILE = "/tmp/config.yaml";

        constexpr static const auto YCSB_PROPERTY_KEY = "ycsb";
        constexpr static const auto NODES_INFO = "nodes";
    public:
        static Properties* GetProperties() {
            if (p == nullptr) {
                std::lock_guard lock(mutex);
                if (p == nullptr) {
                    p = std::make_unique<Properties>();
                    try {
                        p->loadConfig(Properties::YAML_CONFIG_FILE);
                    }
                    catch(const YAML::Exception& e) {
                        LOG(ERROR) << Properties::YAML_CONFIG_FILE << " not exist, switch to bk file!";
                        try {
                            p->loadConfig(Properties::BK_YAML_CONFIG_FILE);
                        }
                        catch(const YAML::Exception& e) {
                            CHECK(false) << Properties::BK_YAML_CONFIG_FILE << " not exist!";
                        }
                    }
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

        std::vector<NodeConfig> getNodesInfo() const {
            std::vector<NodeConfig> ret;
            for (const auto& it: n[NODES_INFO]) {
                NodeConfig cfg;
                cfg.nodeId = it["node_id"].as<int>();
                cfg.groupId = it["group_id"].as<int>();
                cfg.ski = it["ski"].as<std::string>();
                cfg.ip = it["ski"].as<std::string>();
                ret.push_back(cfg);
            }
            return ret;
        }

    protected:
        void loadConfig(const std::string& fileName) {
            n = YAML::LoadFile(fileName);
        }

    private:
        YAML::Node n;
    };
}
