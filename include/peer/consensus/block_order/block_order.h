//
// Created by user on 23-9-14.
//

#pragma once

namespace peer::consensus {
    class BlockOrderInterface {
    public:
        virtual ~BlockOrderInterface() = default;

        virtual bool voteNewBlock(int chainId, int blockId) = 0;

        [[nodiscard]] virtual bool isLeader() const = 0;

        [[nodiscard]] virtual bool waitUntilRaftReady() const = 0;

    };
}