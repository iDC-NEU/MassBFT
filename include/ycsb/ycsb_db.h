//
// Created by user on 23-5-4.
//

#ifndef NBP_YCSB_DB_H
#define NBP_YCSB_DB_H

#include "ycsb/core/db.h"
#include "ycsb/core/status.h"

namespace ycsb::client {
    class YCSB_DB: public ycsb::core::DB {

    public:
        YCSB_DB() {
            LOG(INFO) << "Created a connection to DataBase.";
        }

        core::Status read(const std::string& table, const std::string& key,
                          const std::vector<std::string>& fields,
                          utils::ByteIteratorMap& result) override;

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

#endif //NBP_YCSB_DB_H
