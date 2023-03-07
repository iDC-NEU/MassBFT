//
// Created by user on 23-3-7.
//

#pragma once

#include "proto/transaction.h"
#include "common/phmap.h"

namespace peer::cc {

    class WBReserveTable {
    public:
        WBReserveTable() = default;

        virtual ~WBReserveTable() = default;

        WBReserveTable(const WBReserveTable &) = delete;

        WBReserveTable(WBReserveTable &&) = delete;

        void reset() {
            rsTable.clear();
            cmtTable.clear();
        }

        void reserveWrites(const proto::KVList &writes, std::shared_ptr<const proto::tid_type> transactionID) {
            for (const auto &write: writes) {
                // Skip if the tid in the map is smaller than the current one
                auto exist = [&](TableType::value_type &value) {
                    auto ret = proto::CompareTID(*value.second, *transactionID);
                    // DLOG_IF(INFO, ret != 0) << "same tid invoke twice!";
                    if (ret <= 0) {
                        return;
                    }
                    value.second = transactionID;
                };
                rsTable.try_emplace_l(write->getKeySV(), exist, transactionID);
            }
        }

        [[nodiscard]] bool detectRAW(const proto::KVList &reads, const proto::tid_type &transactionID) const {
            bool raw = false;
            for (const auto &read: reads) {
                rsTable.if_contains_unsafe(read->getKeySV(), [&](const TableType::value_type &v) {
                    if (proto::CompareTID(*v.second, transactionID) < 0) {  // raw dependency
                        raw = true;
                    }
                });
                if (raw) {
                    break;
                }
            }
            return raw;
        }

        void mvccReserveWrites(const proto::KVList &writes, std::shared_ptr<const proto::tid_type> transactionID) {
            for (const auto &write: writes) {
                // Skip if the tid in the map is smaller than the current one
                auto exist = [&](TableType::value_type &value) {
                    auto ret = proto::CompareTID(*value.second, *transactionID);
                    // DLOG_IF(INFO, ret != 0) << "same tid invoke twice!";
                    if (ret >= 0) { // overwrite existing key
                        return;
                    }
                    value.second = transactionID;
                };
                cmtTable.try_emplace_l(write->getKeySV(), exist, transactionID);
            }
        }

        void updateDB(const proto::KVList &writes,
                      const proto::tid_type &transactionID,
                      const std::function<void(std::string_view, std::string_view)>& callback) const {
            for (const auto &write: writes) {
                rsTable.if_contains_unsafe(write->getKeySV(), [&](const TableType::value_type &v) {
                    if (proto::CompareTID(*v.second, transactionID) == 0) {
                        callback(write->getKeySV(), write->getValueSV());
                    }
                });
            }
        }

    private:
        using TableType = util::MyFlatHashMap<std::string_view, std::shared_ptr<const proto::tid_type>, std::mutex>;

        TableType rsTable;
        TableType cmtTable;
    };
}

