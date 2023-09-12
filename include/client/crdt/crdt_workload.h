//
// Created by user on 23-8-13.
//

#pragma once

#include "client/core/workload.h"
#include "client/core/db.h"
#include "client/core/status.h"
#include "client/core/generator/discrete_generator.h"
#include "client/core/common/random_uint64.h"
#include "client/core/common/random_double.h"
#include "client/crdt/crdt_property.h"

namespace client::crdt {
    class CrdtWorkload: public core::Workload {
    public:
        CrdtWorkload() = default;

        void init(const ::util::Properties& prop) override {
            auto n = CrdtProperties::NewFromProperty(prop);
            votingCandidateChooser = std::make_unique<utils::RandomUINT64>(0, n->getCandidateCount());
            votesChooser = std::make_unique<utils::RandomUINT64>(0, 10);
        }

        bool doTransaction(core::DB* db) const override {
            return doVoteCandidate(db);
        }

        bool doInsert(core::DB*) const override { return false; }

    protected:
        bool doVoteCandidate(core::DB* db) const {
            auto candidate = std::to_string(votingCandidateChooser->nextValue());
            auto votes = int(votesChooser->nextValue());
            std::string rawValue;
            zpp::bits::out out(rawValue);
            if (failure(out(candidate, votes))) {
                return false;
            }
            auto status = db->sendInvokeRequest(StaticConfig::VOTING_CHAINCODE_NAME, StaticConfig::VOTING_VOTE, rawValue);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
            return true;
        }

        bool doGetCandidate(core::DB* db) const {
            auto candidate = std::to_string(votingCandidateChooser->nextValue());
            auto status = db->sendInvokeRequest(StaticConfig::VOTING_CHAINCODE_NAME, StaticConfig::VOTING_GET, candidate);
            measurements->beginTransaction(status.getDigest(), status.getGenTimeMs());
            return true;
        }

    private:
        std::unique_ptr<utils::RandomUINT64> votingCandidateChooser;
        std::unique_ptr<utils::RandomUINT64> votesChooser;
    };
}