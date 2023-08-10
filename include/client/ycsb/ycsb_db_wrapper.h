//
// Created by user on 23-8-10.
//

#pragma once

#include "client/core/db.h"

namespace client::ycsb {
    class DBWrapper {
    public:
        struct InvokeRequestType {
            constexpr static const auto YCSB = "ycsb";
            constexpr static const auto UPDATE = "u";
            constexpr static const auto INSERT = "i";
            constexpr static const auto READ = "r";
            constexpr static const auto DELETE = "d";
            constexpr static const auto SCAN = "s";
            constexpr static const auto READ_MODIFY_WRITE = "m";
        };

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