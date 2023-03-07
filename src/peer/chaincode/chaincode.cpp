//
// Created by user on 23-3-7.
//

#include "peer/chaincode/chaincode.h"
#include "common/property.h"

#include "peer/chaincode/simple_transfer.h"

namespace peer::chaincode {
    std::unique_ptr<Chaincode> NewChaincodeByName(const std::string &ccName, std::unique_ptr<ORM> orm) {
        if (ccName == "transfer") {
            return std::make_unique<peer::chaincode::SimpleTransfer>(std::move(orm));
        }
        LOG(ERROR) << "No matched chaincode found!";
        return nullptr;
    }

    std::unique_ptr<Chaincode> NewChaincode(std::unique_ptr<ORM> orm) {
        auto ccName = util::Properties::GetProperties()->getDefaultChaincodeName();
        return NewChaincodeByName(ccName, std::move(orm));
    }

}