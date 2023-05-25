//
// Created by user on 23-5-25.
//

#include "ycsb/core/db.h"

namespace ycsb::core {

    DB::~DB() = default;

    std::unique_ptr<DB> DB::NewDB(const std::string &dbName, const utils::YCSBProperties &n) {
        return nullptr;
    }
}