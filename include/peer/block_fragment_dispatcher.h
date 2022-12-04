//
// Created by peng on 12/1/22.
//

#pragma once

#include "block_fragment_generator.h"
#include "common/bccsp.h"

namespace peer {
    // Active object.
    // BlockFragmentDispatcher is responsible for:
    // 1. receive fragments of multiple blocks from other peers.
    // 2. assemble fragments into blocks pieces.
    // 3. merge block pieces into block.
    class BlockFragmentDispatcher {
    public:
        struct Config {
            struct RegionInfo {
                // the region ski
                std::string ski{};
                int byzantinePeerCount = 0;
                // a list of peer ski
                std::vector<std::string> peerList{};
                // Instance for parallel processing
                // must be equal for all regions
                int instanceCount = 1;
                // ---- these params below is set by dispatcher ----
            };
            // current peer ski
            std::string ski;
            std::vector<RegionInfo> regionList;
        };
        // use an LRU to cache validated root
        inline void addValidatedRoot(const pmt::hashString& root) {

        }

        ~BlockFragmentDispatcher() = default;

        BlockFragmentDispatcher(const BlockFragmentDispatcher&) = delete;

    protected:
        BlockFragmentDispatcher() {


        }
    private:

    };
}
