//
// Created by peng on 11/6/22.
//

#pragma once

#include "ycsb/core/common/byte_iterator.h"
#include "ycsb/core/common/ycsb_property.h"

#include "proto/block.h"

namespace ycsb::core {
    class Status;
    class DB {
    public:
        static std::unique_ptr<DB> NewDB(const std::string & dbName, const utils::YCSBProperties &n);

        virtual ~DB();

        // fields: The list of fields to read, or null for all of them
        virtual core::Status read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) = 0;

        // fields: The list of fields to read, or null for all of them
        virtual core::Status scan(const std::string& table, const std::string& startkey, uint64_t recordcount, const std::vector<std::string>& fields) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status readModifyWrite(const std::string& table,
                                             const std::string& key,
                                             const std::vector<std::string>& readFields,
                                             const utils::ByteIteratorMap& writeValues) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status insert(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) = 0;

        virtual core::Status remove(const std::string& table, const std::string& key) = 0;

        virtual std::unique_ptr<proto::Block> getBlock(int blockNumber) = 0;
    };
}

