//
// Created by user on 23-3-7.
//

#include "peer/chaincode/chaincode.h"
#include "common/property.h"

#include "peer/chaincode/simple_transfer.h"
#include "peer/chaincode/simple_session_store.h"
#include "peer/chaincode/ycsb_chaincode.h"
#include "peer/chaincode/ycsb_row_level.h"
#include "peer/chaincode/hash_chaincode.h"
#include "peer/chaincode/tpcc_chaincode.h"
#include "peer/chaincode/small_bank_chaincode.h"

namespace peer::chaincode {
    std::unique_ptr<Chaincode> NewChaincodeByName(std::string_view ccName, std::unique_ptr<ORM> orm) {
        if (ccName == "ycsb") {
            // return std::make_unique<peer::chaincode::YCSBChaincode>(std::move(orm));
            return std::make_unique<peer::chaincode::YCSBRowLevel>(std::move(orm));
        }
        if (ccName == "tpcc") {
            return std::make_unique<peer::chaincode::TPCCChaincode>(std::move(orm));
        }
        if (ccName == "transfer") {
            return std::make_unique<peer::chaincode::SimpleTransfer>(std::move(orm));
        }
        if (ccName == "session_store") {
            return std::make_unique<peer::chaincode::SimpleSessionStore>(std::move(orm));
        }
        if (ccName == "hash_chaincode") {
            return std::make_unique<peer::chaincode::HashChaincode>(std::move(orm));
        }
        if (ccName == "small_bank") {
            return std::make_unique<peer::chaincode::SmallBankChaincode>(std::move(orm));
        }
        LOG(ERROR) << "No matched chaincode found!";
        return nullptr;
    }

}