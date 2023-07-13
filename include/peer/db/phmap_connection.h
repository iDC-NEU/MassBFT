//
// Created by user on 23-7-13.
//

#pragma once

#include "common/phmap.h"

namespace peer::db {
    class PHMapConnection {
    public:
        struct WriteBatch {
            void Put(std::string key, std::string value) {
                writes.emplace_back(std::move(key), std::move(value));
            }

            void Delete(std::string key) {
                deletes.push_back(std::move(key));
            }

            std::vector<std::pair<std::string, std::string>> writes;
            std::vector<std::string> deletes;
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
                                     [this](auto&& it) { return syncPut(std::move(it.first), std::move(it.second)); })) {
                return false;
            }

            for (auto& it: batch.deletes) {
                syncDelete(std::move(it));
            }
            return true;
        }

        bool syncPut(auto&& key, auto&& value) {
            auto exist = [&](TableType::value_type &v) {
                v.second = std::forward<decltype(value)>(value);
            };
            db.try_emplace_l(std::forward<decltype(key)>(key), exist, std::forward<decltype(value)>(value));
            return true;
        }

        inline bool asyncPut(auto&& key, auto&& value) {
            return syncPut(std::forward<decltype(key)>(key), std::forward<decltype(value)>(value));
        }

        bool get(auto&& key, std::string* value) const {
            auto ret = db.if_contains(std::forward<decltype(key)>(key), [&](const TableType::value_type &v) {
                *value = v.second;
            });
            return ret;
        }

        // It is not an error if "key" did not exist in the database.
        bool syncDelete(auto&& key) {
            db.erase(std::forward<decltype(key)>(key));
            return true;
        }

        inline bool asyncDelete(auto&& key) {
            return syncDelete(std::forward<decltype(key)>(key));
        }

    protected:
        PHMapConnection() = default;

    private:
        std::string _dbName;
        using TableType = util::MyFlatHashMap<std::string, std::string, std::mutex>;
        TableType db;
    };
}