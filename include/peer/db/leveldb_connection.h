//
// Created by peng on 2/20/23.
//

#pragma once

#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include "glog/logging.h"
#include <memory>

namespace peer::db {
    class LeveldbConnection {
    public:
        LeveldbConnection(const LeveldbConnection&) = delete;

        LeveldbConnection(LeveldbConnection&&) = delete;

        virtual ~LeveldbConnection() = default;

        // create a new db connection
        static std::unique_ptr<LeveldbConnection> NewLeveldbConnection(const std::string& dbName) {
            leveldb::Options options;
            options.create_if_missing = true;
            leveldb::DB* db;
            leveldb::Status status = leveldb::DB::Open(options, dbName, &db);
            if (!status.ok()) {
                LOG(WARNING) << "Can not open database: " << dbName;
                return nullptr;
            }
            std::unique_ptr<LeveldbConnection> dbc(new LeveldbConnection());
            dbc->dbName = dbName;
            dbc->db.reset(db);
            return dbc;
        }

        [[nodiscard]] const std::string& getDBName() const { return dbName; }

        bool asyncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(asyncWrite,
                                  leveldb::Slice{key.data(), key.size()},
                                  leveldb::Slice{value.data(), value.size()});
            return status.ok();
        }

        bool syncPut(std::string_view key, std::string_view value) {
            auto status = db->Put(syncWrite,
                                  leveldb::Slice{key.data(), key.size()},
                                  leveldb::Slice{value.data(), value.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool asyncDelete(std::string_view key) {
            auto status = db->Delete(asyncWrite, leveldb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // It is not an error if "key" did not exist in the database.
        bool syncDelete(std::string_view key) {
            auto status = db->Delete(syncWrite, leveldb::Slice{key.data(), key.size()});
            return status.ok();
        }

        // batch.Put(key, value);
        // batch.Delete(key);
        bool syncWriteBatch(const std::function<bool(leveldb::WriteBatch* batch)>& callback) {
            leveldb::WriteBatch batch;
            if (!callback(&batch)) {
                return false;
            }
            auto status = db->Write(syncWrite, &batch);
            return status.ok();
        }

        bool get(std::string_view key, std::string* value) const {
            leveldb::Status status = db->Get(leveldb::ReadOptions(), leveldb::Slice{key.data(), key.size()}, value);
            return status.ok();
        }

    protected:
        LeveldbConnection() {
            syncWrite.sync = true;
            asyncWrite.sync= false;
        }

    private:
        leveldb::WriteOptions syncWrite;
        leveldb::WriteOptions asyncWrite;
        std::string dbName;
        std::unique_ptr<leveldb::DB> db;
    };

}