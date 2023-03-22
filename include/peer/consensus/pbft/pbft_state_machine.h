//
// Created by user on 23-3-21.
//

#pragma once

#include <string>
#include <memory>
#include "proto/block.h"

namespace peer::consensus {
    class PBFTStateMachine {
    public:
        virtual ~PBFTStateMachine() = default;
        // When a node become follower, it verifies proposal sent from the leader
        virtual bool OnVerifyProposal(std::unique_ptr<::proto::Block>) = 0;

        virtual bool OnDeliver(std::unique_ptr<::proto::Block>) = 0;

        virtual void OnLeaderStart(const std::string& context) = 0;

        virtual void OnLeaderStop(const std::string& context) = 0;

        virtual std::shared_ptr<::proto::Block> OnRequestProposal(int regionId, int blockNumber, const std::string& context) = 0;
    };
}