//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/db.h"

namespace client::timeSeries {
    class DBWrapper {
    public:
        explicit DBWrapper(client::core::DB* db) :db(db) { }

        core::Status read(const std::string& table, const std::string& key, const std::vector<std::string>& fields);

        core::Status scan(const std::string& table, const std::string& startKey, uint64_t recordCount, const std::vector<std::string>& fields);

        core::Status update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values);

        core::Status readModifyWrite(const std::string& table, const std::string& key,
                                     const std::vector<std::string>& readFields, const utils::ByteIteratorMap& writeValues);

        core::Status insert(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values);

        core::Status remove(const std::string& table, const std::string& key);

    private:
        client::core::DB* db;
    };
}