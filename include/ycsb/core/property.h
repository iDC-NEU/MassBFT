//
// Created by peng on 10/18/22.
//

#ifndef BENCHMARKCLIENT_PROPERTY_H
#define BENCHMARKCLIENT_PROPERTY_H

#include <mutex>
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"

namespace ycsb::utils {
    class Properties {
    private:
        static std::unique_ptr<Properties> p;
        static std::mutex mutex;
        constexpr static const auto YAML_CONFIG_FILE = "config.yaml";
        constexpr static const auto BK_YAML_CONFIG_FILE = "/tmp/config.yaml";

    public:
        static YAML::Node* getProperties() {
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
            return p->getNode();
        }
        ~Properties() = default;

    protected:
        void loadConfig(const std::string& fileName) {
            n = YAML::LoadFile(fileName);
        }
        YAML::Node* getNode() {
            return &n;
        }

    private:
        YAML::Node n;
    };
}
#endif //BENCHMARKCLIENT_PROPERTY_H
