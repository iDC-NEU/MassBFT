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

        [[nodiscard]] virtual std::optional<::util::OpenSSLED25519::digestType> OnSignMessage(const ::util::NodeConfigPtr& localNode, const std::string& message) const = 0;
        // When a node become follower, it verifies proposal sent from the leader
        virtual bool OnVerifyProposal(::util::NodeConfigPtr localNode, const std::string& context) = 0;

        virtual bool OnDeliver(::util::NodeConfigPtr localNode,
                               const std::string& context,
                               std::vector<::proto::SignatureString> signatures) = 0;

        virtual void OnLeaderStart(::util::NodeConfigPtr localNode, int sequence) = 0;

        virtual void OnLeaderStop(::util::NodeConfigPtr localNode, int sequence) = 0;

        virtual std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr localNode, int sequence, const std::string& context) = 0;
    };
}