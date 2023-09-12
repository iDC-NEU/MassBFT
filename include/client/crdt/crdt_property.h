//
// Created by user on 23-5-24.
//

#pragma once

#include "client/core/default_property.h"

namespace client::crdt {
    class CrdtProperties : public core::BaseProperties<CrdtProperties> {
    public:
        constexpr static const auto PROPERTY_NAME = "crdt";

        constexpr static const auto CANDIDATE_COUNT_PROPERTY = "candidate_count";

    public:
        inline auto getCandidateCount() const {
            return n[CANDIDATE_COUNT_PROPERTY].as<int>(1000000);
        }

        explicit CrdtProperties(const YAML::Node& node) : core::BaseProperties<CrdtProperties>(node) { }
    };

    class StaticConfig {
    public:
        constexpr static const auto VOTING_CHAINCODE_NAME = "vote";
        constexpr static const auto VOTING_VOTE = "vote";
        constexpr static const auto VOTING_GET = "get";
    };
}