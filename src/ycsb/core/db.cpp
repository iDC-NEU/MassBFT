//
// Created by user on 23-5-25.
//

#include "ycsb/core/db.h"
#include "ycsb/neuchain_db.h"

namespace ycsb::core {

    DB::~DB() = default;

    std::unique_ptr<DB> DB::NewDB(const std::string &dbName, const utils::YCSBProperties &n) {
        if(dbName == "neuChain"){
            return std::make_unique<client::NeuChainDB>();
        }
        return nullptr;
    }
}