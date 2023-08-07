//
// Created by peng on 2/21/23.
//

#pragma once

#include "proto/read_write_set.h"
#include "peer/db/db_interface.h"
#include "zpp_bits.h"

#include <functional>

namespace peer::chaincode {
    class ORM {
    public:
        ~ORM() = default;

        ORM(const ORM&) = delete;

        ORM(ORM&&) = delete;

        static std::unique_ptr<ORM> NewORMFromDBInterface(const db::DBConnection* dbInstance) {
            std::unique_ptr<ORM> orm(new ORM());
            orm->getFromDB = [dbInstance](auto && PH1, auto && PH2) {
                return dbInstance->get(std::forward<decltype(PH1)>(PH1), std::forward<decltype(PH2)>(PH2));
            };
            return orm;
        }

        [[nodiscard]] inline bool get(const std::string& key, std::string_view* valueSV) {
            return get(std::string(key), valueSV);
        }

        [[nodiscard]] bool get(std::string&& key, std::string_view* valueSV) {
            std::string value;
            auto ret = getFromDB(key, &value);
            // empty value is marked deleted
            if (!ret || value.empty()) {
                return false;
            }
            std::unique_ptr<proto::KV> readKV(new proto::KV());
            readKV->setKey(std::move(key));
            readKV->setValue(std::move(value));
            *valueSV = readKV->getValueSV();
            reads.push_back(std::move(readKV));
            return true;
        }

        inline void put(const std::string& key, const std::string& value) {
            put(std::string(key), std::string(value));
        }

        inline void put(const std::string& key, std::string&& value) {
            put(std::string(key), std::move(value));
        }

        inline void put(std::string&& key, const std::string& value) {
            put(std::move(key), std::string(value));
        }

        void put(std::string&& key, std::string&& value) {
            std::unique_ptr<proto::KV> writeKV(new proto::KV());
            writeKV->setKey(std::move(key));
            writeKV->setValue(std::move(value));
            writes.push_back(std::move(writeKV));
        }

        void del(const std::string& key) {
            del(std::string(key));
        }

        void del(std::string&& key) {
            std::unique_ptr<proto::KV> writeKV(new proto::KV());
            writeKV->setKey(std::move(key));
            writes.push_back(std::move(writeKV));
        }

        // set the return string
        void setResult(auto&& result_) { result = std::forward<decltype(result_)>(result_); }

        // return the rwSets along with the return string
        std::string reset(proto::KVList& reads_, proto::KVList& writes_) {
            reads_ = std::move(reads);
            writes_ = std::move(writes);
            return std::move(result);
        }

        void reset() {
            reads.clear();
            writes.clear();
            result.clear();
        }

    protected:
        ORM() = default;

    private:
        proto::KVList reads;
        proto::KVList writes;
        std::string result;
        // ORM should not modify db state
        std::function<bool(std::string_view key, std::string* value)> getFromDB;
    };

}