//
// Created by user on 23-7-4.
//

#include "common/property.h"
#include <fstream>
#include <thread>

namespace util {
    void NodeProperties::setSingleNodeInfo(const NodeConfig &cfg)  {
        auto groupKey = BuildGroupKey(cfg.groupId);
        auto list = n[groupKey];
        // search the key
        for (auto && it : list) {
            try {
                if (it["ski"].as<std::string>() == cfg.ski) {
                    it["node_id"] = cfg.nodeId;
                    it["group_id"] = cfg.groupId;
                    it["pri_ip"] = cfg.priIp;
                    it["pub_ip"] = cfg.pubIp;
                    return;
                }
            } catch (const YAML::Exception &e) {
                LOG(INFO) << "Can not load ski: " << e.what();
            }
        }
        // insert a new key
        YAML::Node node;
        node["node_id"] = cfg.nodeId;
        node["group_id"] = cfg.groupId;
        node["ski"] = cfg.ski;
        node["pri_ip"] = cfg.priIp;
        node["pub_ip"] = cfg.pubIp;
        list.push_back(node);
    }

    bool Properties::LoadProperties(const std::string &fileName) {
        properties = NewProperties(fileName);
        if (properties == nullptr) {
            return false;
        }
        return true;
    }

    std::unique_ptr<Properties> Properties::NewProperties(const std::string &fileName) {
        auto prop = std::make_unique<Properties>();
        if (fileName.empty()) {
            YAML::Node node;
            node[CHAINCODE_PROPERTIES] = {};
            node[NODES_PROPERTIES] = {};
            prop->_node = node;
            return prop;
        }
        try {
            auto node = YAML::LoadFile(fileName);
            if (!node[CHAINCODE_PROPERTIES].IsDefined() || node[CHAINCODE_PROPERTIES].IsNull()) {
                LOG(ERROR) << "CHAINCODE_PROPERTIES not exist.";
                return nullptr;
            }
            if (!node[NODES_PROPERTIES].IsDefined() || node[NODES_PROPERTIES].IsNull()) {
                LOG(ERROR) << "NODES_PROPERTIES not exist.";
                return nullptr;
            }
            prop->_node = node;
            return prop;
        } catch (const YAML::Exception &e) {
            LOG(ERROR) << "Can not load config: " << e.what();
        }
        return nullptr;
    }

    bool Properties::SaveProperties(const std::string &fileName) {
        if (properties == nullptr) {
            return false;
        }
        return SaveProperties(fileName, *properties);
    }

    bool Properties::SaveProperties(const std::string &fileName, const Properties &prop) {
        YAML::Emitter emitter;
        emitter << prop._node;
        if (!emitter.good()) {
            return false;
        }
        std::ofstream file(fileName);
        file << emitter.c_str();
        file.close();
        return true;
    }

    int Properties::getAriaWorkerCount() const {
        auto workerCount = std::max((int)std::thread::hardware_concurrency() / 4, 1);
        try {
            return _node[ARIA_WORKER_COUNT].as<int>();
        } catch (const YAML::Exception &e) {
            LOG(INFO) << "Can not find ARIA_WORKER_COUNT, leave it to " << workerCount << ".";
        }
        return workerCount;
    }

    int Properties::getBCCSPWorkerCount() const {
        auto workerCount = std::max((int)std::thread::hardware_concurrency() * 7 / 8, 1);
        try {
            return _node[BCCSP_WORKER_COUNT].as<int>();
        } catch (const YAML::Exception &e) {
            LOG(INFO) << "Can not find BCCSP_WORKER_COUNT, leave it to " << workerCount << ".";
        }
        return workerCount;
    }
}