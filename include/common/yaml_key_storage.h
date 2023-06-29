//
// Created by user on 23-5-8.
//

#pragma once

#include "common/bccsp.h"
#include "yaml-cpp/yaml.h"

namespace util {
    class YAMLKeyStorage : public util::KeyStorage {
    public:
        explicit YAMLKeyStorage(const YAML::Node& node) {
            keyMap = node;
        }

        bool saveKey(std::string_view ski, std::string_view raw, bool isPrivate, bool overwrite) override {
            std::unique_lock guard(mutex);
            std::string skiStr(ski);
            if (!overwrite) {
                if (keyMap[skiStr].IsDefined() && !keyMap[skiStr].IsNull()) {
                    return false;
                }
            }
            YAML::Node node;
            node["raw"] = OpenSSL::bytesToString(raw);
            node["private"] = isPrivate;
            keyMap[skiStr] = node;
            return true;
        }

        auto loadKey(std::string_view ski) -> std::optional<std::pair<std::string, bool>> override {
            std::unique_lock guard(mutex);
            std::pair<std::string, bool> ret;
            try {
                std::string skiStr(ski);
                auto node = keyMap[skiStr];
                ret.first = OpenSSL::stringToBytes(node["raw"].as<std::string>());
                ret.second = node["private"].as<bool>();
            } catch (const YAML::Exception &e) {
                LOG(WARNING) << "Can not load key: " << e.what();
                return std::nullopt;
            }
            return ret;
        }

    private:
        std::mutex mutex;
        YAML::Node keyMap;
    };
}