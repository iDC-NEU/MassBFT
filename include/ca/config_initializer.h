//
// Created by user on 23-7-7.
//

#pragma once

#include <vector>
#include <string>

namespace ca {
    /* The initializer is responsible for initializing the public and private keys of the node
     * and generating the default configuration file.
     * In subsequent versions, the initializer will also be responsible for tasks
     * such as distributing binary files. */
    class Initializer {
    public:
        explicit Initializer(std::vector<int> groupNodeCount);

        static void SetLocalId(int groupId, int nodeId);

        static bool SetNodeIp(int groupId, int nodeId, const std::string& pub, const std::string& pri);

        bool initDefaultConfig();

        static bool SaveConfig(const std::string &fileName);

    private:
        const std::vector<int> _groupNodeCount;
    };
}