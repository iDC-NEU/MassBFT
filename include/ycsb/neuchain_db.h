//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_NEUCHAIN_DB_H
#define NEUCHAIN_PLUS_NEUCHAIN_DB_H

#include "ycsb/core/db.h"
#include "ycsb/core/status.h"

namespace ycsb::client {
    class NeuChainDB: public ycsb::core::DB {

    public:
        NeuChainDB() {
            LOG(INFO) << "Created a connection to NeuChainDB.";
        }

        core::Status read(const std::string& table, const std::string& key,
                          const std::vector<std::string>& fields,
                          utils::ByteIteratorMap& result) override {
            // TODO: send a request to blockchain system.
            return core::STATUS_OK;
        }

        core::Status scan(const std::string& table, const std::string& startkey, uint64_t recordcount,
                                           const std::vector<std::string>& fields,
                                           std::vector<utils::ByteIteratorMap>& result) override {
            return core::STATUS_OK;
        }

        core::Status update(const std::string& table, const std::string& key,
                                             const utils::ByteIteratorMap& values) override {
            return core::STATUS_OK;
        }

        core::Status insert(const std::string& table, const std::string& key,
                                             const utils::ByteIteratorMap& values) override {
            return core::STATUS_OK;

        }

        core::Status remove(const std::string& table, const std::string& key) override {
            return core::STATUS_OK;
        }
    };
}

#endif //NEUCHAIN_PLUS_NEUCHAIN_DB_H
