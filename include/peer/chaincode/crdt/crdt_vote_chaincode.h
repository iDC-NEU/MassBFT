//
// Created by user on 23-7-12.
//

#pragma once

#include "peer/chaincode/crdt/crdt_chaincode.h"
#include "client/crdt/crdt_property.h"
#include "client/core/generator/generator.h"

namespace peer::crdt::chaincode {
    class VoteChaincode : public CrdtChaincode {
    public:
        explicit VoteChaincode(std::unique_ptr<CrdtORM> orm) : CrdtChaincode(std::move(orm)) { }

        int InitDatabase() override {
            ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
            auto* property = util::Properties::GetProperties();
            auto crdtProperties = client::crdt::CrdtProperties::NewFromProperty(*property);
            auto candidateCount = crdtProperties->getCandidateCount();
            for (int i=0; i<candidateCount; i++) {
                std::string candidateName = std::to_string(i);
                auto callback = [&](std::string& rawValue) {
                    zpp::bits::out out(rawValue);
                    if (failure(out(int(0)))) {
                        return false;
                    }
                    return true;
                };
                if (!orm->put(candidateName, callback)) {
                    return -1;
                }
            }
            return 0;
        }

        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override {
            if (funcNameSV == client::crdt::StaticConfig::VOTING_GET) {
                return Get(std::string(argSV));
            }
            if(funcNameSV == client::crdt::StaticConfig::VOTING_VOTE) {
                zpp::bits::in in(argSV);
                std::string key;
                int count;
                if (failure(in(key, count))) {
                    return -1;
                }
                return Vote(key, count);
            }
            return -1;
        }

        int Vote(const std::string& candidate, int count) {
            auto callback = [&](std::string& rawValue) {
                int oldCount;
                {
                    zpp::bits::in in(rawValue);
                    if (failure(in(oldCount))) {
                        return false;
                    }
                }
                {
                    zpp::bits::out out(rawValue);
                    if (failure(out(oldCount + count))) {
                        return false;
                    }
                }
                return true;
            };
            if (!orm->put(candidate, callback)) {
                return -1;
            }
            return 0;
        }

        int Get(const std::string& candidate) {
            std::string value;
            if (orm->get(candidate, value)) {
                orm->setResult(std::move(value));
                return 0;
            }
            return -1;
        }
    };
}