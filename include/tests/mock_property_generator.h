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
            util::Properties::LoadProperties(); // init
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
                    nodeProperties.setSingleNodeInfo(cfg);
                }
            }
            std::filesystem::path newDir = "/home/user/nc_bft";
            std::filesystem::current_path(newDir);
        }

        static void SetLocalId(int groupId, int nodeId) {
            auto* properties = util::Properties::GetProperties();
            auto nodeProperties = properties->getNodeProperties();
            nodeProperties.setLocalNodeInfo(groupId, nodeId);
        }
    };
}