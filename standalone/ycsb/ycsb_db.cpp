//
// Created by user on 23-5-4.
//

#include "ycsb/ycsb_db.h"
#include "glog/logging.h"
#include "proto/p"

struct Request {
    // if empty = test_table
    std::string tableName;
    // if empty = use config file default
    std::string funcName;
    std::vector<std::string> reads;
    std::vector<std::string> writes;
};

ycsb::core::Status ycsb::client::YCSB_DB::read(const std::string &table, const std::string &key,
                                               const std::vector<std::string> &fields, utils::ByteIteratorMap &result) {
    Request request;
    request.funcName = "read";
    request.tableName = table;
    YCSB_FOR_BLOCK_BENCH

    return core::STATUS_OK;
}