//
// Created by peng on 2/20/23.
//

#pragma once

#include "rocksdb/db.h"

#include "glog/logging.h"
#include <memory>

namespace peer::db {
    class RocksdbConnection {
    public:
        RocksdbConnection(const RocksdbConnection&) = delete;

        RocksdbConnection(RocksdbConnection&&) = delete;

        virtual ~RocksdbConnection() = default;

        // create a new db connection
        static std::unique_ptr<RocksdbConnection> NewConnection(const std::string& dbName) {
            rocksdb::Options options;
            options.create_if_missing = true;
            rocksdb::DB* db;
            rocksdb::Status status = rocksdb::DB::Open(options, dbName, &db);
            if (!status.ok()) {
                LOG(WARNING) << "Can not open database: " << dbName;
                return nullptr;
            }
            std::unique_ptr<RocksdbConnection> dbc(new RocksdbConnection());
            dbc->dbName = dbName;
            dbc->db.reset(db);
            return dbc;
        }

        [[nodiscard]] const std::string& getDBName() const { return dbName; }

        bool asyncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(asyncWrite,
                                  rocksdb::Slice{key.data(), key.size()},
                                  rocksdb::Slice{value.data(), value.size()});
            return status.ok();
        }

        bool syncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(syncWrite,
                                  rocksdb::Slice{key.data(), key.size()},
                                  rocksdb::Slice{value.data(), value.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool asyncDelete(std::string_view key) {
            auto status = db->Delete(asyncWrite, rocksdb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool syncDelete(std::string_view key) {
            auto status = db->Delete(syncWrite, rocksdb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // batch.Put(key, value);
        // batch.Delete(key);
        bool syncWriteBatch(const std::function<bool(rocksdb::WriteBatch* batch)>& callback) {
            rocksdb::WriteBatch batch;
            if (!callback(&batch)) {
                return false;
            }
            auto status = db->Write(syncWrite, &batch);
            return status.ok();
        }

        bool get(std::string_view key, std::string* value) const {
            rocksdb::Status status = db->Get(rocksdb::ReadOptions(), rocksdb::Slice{key.data(), key.size()}, value);
            return status.ok();
        }

    protected:
        RocksdbConnection() {
            syncWrite.sync = true;
            asyncWrite.sync= false;
        }

    private:
        rocksdb::WriteOptions syncWrite;
        rocksdb::WriteOptions asyncWrite;
        std::string dbName;
        std::unique_ptr<rocksdb::DB> db;
    };

}