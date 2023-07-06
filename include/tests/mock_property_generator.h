//
// Created by user on 23-5-11.
//

#pragma once

#include <filesystem>
#include "common/property.h"

namespace tests {

    class MockPropertyGenerator {
    public:
        static void GenerateDefaultProperties(int groupCount, int nodesPerGroup) {
            CHECK(util::Properties::LoadProperties()); // init
            auto* properties = util::Properties::GetProperties();
            // init nodes
            auto nodeProperties = properties->getNodeProperties();
            auto bccspProperties = properties->getCustomProperties("bccsp");

            util::NodeConfigPtr cfg(new util::NodeConfig);
            for (int i=0; i<groupCount; i++) {
                for (int j=0; j<nodesPerGroup; j++) {
                    cfg->nodeId = j;
                    cfg->groupId = i;
                    cfg->ski = std::to_string(i) + "_" + std::to_string(j);
                    cfg->priIp = "127.0.0." + std::to_string(j+1);
                    cfg->pubIp = "127.0.0." + std::to_string(j+1);
                    nodeProperties.setSingleNodeInfo(*cfg);
                    // init bccsp
                    YAML::Node node;
                    node["raw"] = "1498b5467a63dffa2dc9d9e069caf075d16fc33fdd4c3b01bfadae6433767d93";
                    node["private"] = true;
                    bccspProperties[cfg->ski] = node;
                }
            }
        }

        static void SetLocalId(int groupId, int nodeId) {
            auto* properties = util::Properties::GetProperties();
            auto nodeProperties = properties->getNodeProperties();
            nodeProperties.setLocalNodeInfo(groupId, nodeId);
        }
    };
}