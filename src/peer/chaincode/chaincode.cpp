//
// Created by user on 23-3-7.
//

#include "peer/chaincode/chaincode.h"
#include "common/property.h"

#include "peer/chaincode/simple_transfer.h"
#include "peer/chaincode/simple_session_store.h"
#include "peer/chaincode/ycsb_chaincode.h"

namespace peer::chaincode {
    std::unique_ptr<Chaincode> NewChaincodeByName(const std::string &ccName, std::unique_ptr<ORM> orm) {
        if (ccName == "transfer") {
            return std::make_unique<peer::chaincode::SimpleTransfer>(std::move(orm));
        }
        if (ccName == "session_store") {
            return std::make_unique<peer::chaincode::SimpleSessionStore>(std::move(orm));
        }
        if (ccName == "ycsb") {
            return std::make_unique<peer::chaincode::YCSBChainCode>(std::move(orm));
        }
        LOG(ERROR) << "No matched chaincode found!";
        return nullptr;
    }

}