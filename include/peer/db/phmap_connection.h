//
// Created by user on 23-7-13.
//

#pragma once

#include "common/phmap.h"

namespace peer::db {
    class PHMapConnection {
    public:
        struct WriteBatch {
            void Put(std::string_view key, std::string_view value) {
                writes.emplace_back(key, value);
            }

            void Delete(std::string_view key) {
                deletes.push_back(key);
            }

            std::vector<std::pair<std::string_view, std::string_view>> writes;
            std::vector<std::string_view> deletes;
        };

        static std::unique_ptr<PHMapConnection> NewConnection(const std::string& dbName) {
            auto db = std::unique_ptr<PHMapConnection>(new PHMapConnection);
            db->_dbName = dbName;
            return db;
        }

        [[nodiscard]] const std::string& getDBName() const { return _dbName; }

        bool syncWriteBatch(const std::function<bool(WriteBatch* batch)>& callback) {
            WriteBatch batch;
            if (!callback(&batch)) {
                return false;
            }

            if (!std::ranges::all_of(batch.writes.begin(),
                                     batch.writes.end(),
                                     [this](auto&& it) { return syncPut(it.first, it.second); })) {
                return false;
            }

            for (auto& it: batch.deletes) {
                syncDelete(it);
            }
            return true;
        }

        bool syncPut(std::string_view key, std::string_view value) {
            auto exist = [&](TableType::value_type &v) {
                v.second = value;
            };
            return db.try_emplace_l(key, exist, value);
        }

        inline bool asyncPut(std::string_view key, std::string_view value) { return syncPut(key, value); }

        bool get(std::string_view key, std::string* value) const {
            return db.if_contains_unsafe(key, [&](const TableType::value_type &v) {
                *value = v.second;
            });
        }

        // It is not an error if "key" did not exist in the database.
        bool syncDelete(std::string_view key) {
            db.erase(key);
            return true;
        }

        inline bool asyncDelete(std::string_view key) { return syncDelete(key); }

    protected:
        PHMapConnection() = default;

    private:
        std::string _dbName;
        using TableType = util::MyFlatHashMap<std::string, std::string, std::mutex>;
        TableType db;
    };
}