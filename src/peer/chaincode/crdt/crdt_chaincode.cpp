//
// Created by user on 23-9-6.
//

#include "peer/chaincode/crdt/crdt_chaincode.h"
#include "peer/chaincode/crdt/crdt_vote_chaincode.h"
#include "client/crdt/crdt_property.h"

namespace peer::crdt::chaincode {
    std::unique_ptr<CrdtChaincode> NewChaincodeByName(std::string_view ccName, std::unique_ptr<CrdtORM> orm) {
        if (client::crdt::StaticConfig::VOTING_CHAINCODE_NAME == ccName) {
            return std::make_unique<VoteChaincode>(std::move(orm));
        }
        return nullptr;
    }
}