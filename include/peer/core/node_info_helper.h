//
// Created by user on 23-5-8.
//

#pragma once

namespace util {
    class Properties;
}

namespace peer::core {

    class NodeInfoHelper {
    public:
        static std::unique_ptr<NodeInfoHelper> NewNodeInfoHelper(std::shared_ptr<util::Properties> properties);
    };
}