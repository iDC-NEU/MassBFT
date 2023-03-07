//
// Created by peng on 2/19/23.
//

#pragma once

#include "proto/transaction.h"
#include "common/phmap.h"

namespace peer::cc {

    // The reserve map of a specific table (or sharded table)
    // key have string view type, must NOT be free before ReserveTable is destroyed
    class ReserveTable {
    public:
        class Dependency {
        public:
            bool waw = false;
            bool war = false;
            bool raw = false;
        };

        ReserveTable() = default;

        virtual ~ReserveTable() = default;

        ReserveTable(const ReserveTable &) = delete;

        ReserveTable(ReserveTable &&) = delete;

        void reset() {
            readTable.clear();
            writeTable.clear();
        }

        void reserveRWSets(const proto::KVList &reads,
                           const proto::KVList &writes,
                           std::shared_ptr<const proto::tid_type> transactionID) {
            for (const auto &read: reads) {
                // Skip if the tid in the map is smaller than the current one
                auto exist = [&](TableType::value_type &value) {
                    auto ret = proto::CompareTID(*value.second, *transactionID);
                    // DLOG_IF(INFO, ret != 0) << "same tid invoke twice!";
                    if (ret <= 0) {
                        return;
                    }
                    value.second = transactionID;
                };
                readTable.try_emplace_l(read->getKeySV(), exist, transactionID);
            }

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
                writeTable.try_emplace_l(write->getKeySV(), exist, transactionID);
            }
        }

        [[nodiscard]] Dependency analysisDependent(const proto::KVList &reads,
                                                   const proto::KVList &writes,
                                                   const proto::tid_type &transactionID) const {
            Dependency dependency;
            for (const auto &write: writes) {
                writeTable.if_contains_unsafe(write->getKeySV(), [&](const TableType::value_type &v) {
                    if (proto::CompareTID(*v.second, transactionID) < 0) {  // waw dependency
                        dependency.waw = true;
                    }
                });
                if (dependency.waw) {
                    break;
                }
            }
            for (const auto &write: writes) {
                readTable.if_contains_unsafe(write->getKeySV(), [&](const TableType::value_type &v) {
                    if (proto::CompareTID(*v.second, transactionID) < 0) {  // war dependency
                        dependency.war = true;
                    }
                });
                if (dependency.war) {
                    break;
                }
            }
            for (const auto &read: reads) {
                writeTable.if_contains_unsafe(read->getKeySV(), [&](const TableType::value_type &v) {
                    if (proto::CompareTID(*v.second, transactionID) < 0) {  // raw dependency
                        dependency.raw = true;
                    }
                });
                if (dependency.raw) {
                    break;
                }
            }
            return dependency;
        }

    private:
        using TableType = util::MyFlatHashMap<std::string_view, std::shared_ptr<const proto::tid_type>, std::mutex>;

        TableType readTable;
        TableType writeTable;
    };
}

