//
// Created by peng on 12/1/22.
//

#pragma once

#include "block_fragment_generator.h"

namespace peer {
    // Active object.
    // BlockFragmentDispatcher is responsible for:
    // 1. receive fragments of multiple blocks from other peers.
    // 2. assemble fragments into blocks pieces.
    // 3. merge block pieces into block.
    class BlockFragmentDispatcher {
    public:
        // use an LRU to cache validated root
        inline void addValidatedRoot(const pmt::hashString& root) {

        }

        ~BlockFragmentDispatcher() = default;

        BlockFragmentDispatcher(const BlockFragmentDispatcher&) = delete;

    };
}
