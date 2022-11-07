//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_DB_H
#define NEUCHAIN_PLUS_DB_H

#include <string>
#include "yaml-cpp/yaml.h"
#include "ycsb/core/common/byte_iterator.h"

namespace ycsb::core {
    class Status;
    class DB {
    public:
        const YAML::Node* properties = nullptr;
        virtual ~DB() = default;
        /**
         * Initialize any state for this DB.
         * Called once per DB instance; there is one DB instance per client thread.
         */
        virtual void init() {}

        /**
         * Cleanup any state for this DB.
         * Called once per DB instance; there is one DB instance per client thread.
         */
        virtual void cleanup() {}

        /**
         * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
         *
         * @param table The name of the table
         * @param key The record key of the record to read.
         * @param fields The list of fields to read, or null for all of them
         * @param result A HashMap of field/value pairs for the result
         * @return The result of the operation.
         */
        virtual core::Status read(const std::string& table, const std::string& key,
                                  const std::vector<std::string>& fields,
                                  utils::ByteIteratorMap& result) = 0;

        /**
         * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored
         * in a HashMap.
         *
         * @param table The name of the table
         * @param startkey The record key of the first record to read.
         * @param recordcount The number of records to read
         * @param fields The list of fields to read, or null for all of them
         * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
         * @return The result of the operation.
         */
        virtual core::Status scan(const std::string& table, const std::string& startkey, uint64_t recordcount,
                                  const std::vector<std::string>& fields,
                                  std::vector<utils::ByteIteratorMap>& result) = 0;

        /**
         * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the
         * record with the specified record key, overwriting any existing values with the same field name.
         *
         * @param table The name of the table
         * @param key The record key of the record to write.
         * @param values A HashMap of field/value pairs to update in the record
         * @return The result of the operation.
         */
        virtual core::Status update(const std::string& table, const std::string& key,
                                    const utils::ByteIteratorMap& values) = 0;

        /**
         * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the
         * record with the specified record key.
         *
         * @param table The name of the table
         * @param key The record key of the record to insert.
         * @param values A HashMap of field/value pairs to insert in the record
         * @return The result of the operation.
         */
        virtual core::Status insert(const std::string& table, const std::string& key,
                                    const utils::ByteIteratorMap& values) = 0;

        /**
         * Delete a record from the database.
         *
         * @param table The name of the table
         * @param key The record key of the record to delete.
         * @return The result of the operation.
         */
        virtual core::Status remove(const std::string& table, const std::string& key) = 0;
    };
}

#endif //NEUCHAIN_PLUS_DB_H
