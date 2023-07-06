//
// Created by user on 23-7-4.
//

#include "common/property.h"
#include <fstream>
#include <thread>

namespace util {
    bool Properties::LoadProperties(const std::string &fileName) {
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

    bool Properties::SaveProperties(const std::string &fileName) {
        if (properties == nullptr) {
            return false;
        }
        YAML::Emitter emitter;
        emitter << properties->_node;
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
}