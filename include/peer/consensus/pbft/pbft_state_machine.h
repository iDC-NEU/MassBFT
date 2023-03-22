//
// Created by user on 23-3-21.
//

#pragma once

#include "common/property.h"
#include "proto/block.h"

#include <string>
#include <memory>

namespace peer::consensus {
    class PBFTStateMachine {
    public:
        virtual ~PBFTStateMachine() = default;
        // When a node become follower, it verifies proposal sent from the leader
        virtual bool OnVerifyProposal(::util::NodeConfigPtr localNode, std::unique_ptr<::proto::Block>) = 0;

        virtual bool OnDeliver(::util::NodeConfigPtr localNode, std::unique_ptr<::proto::Block>) = 0;

        virtual void OnLeaderStart(::util::NodeConfigPtr localNode, const std::string& context) = 0;

        virtual void OnLeaderStop(::util::NodeConfigPtr localNode, const std::string& context) = 0;

        virtual std::shared_ptr<::proto::Block> OnRequestProposal(::util::NodeConfigPtr localNode, int blockNumber, const std::string& context) = 0;
    };
}