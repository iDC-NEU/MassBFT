//
// Created by peng on 11/6/22.
//

#pragma once

#include "ycsb/core/common/byte_iterator.h"
#include "proto/block.h"
#include "common/property.h"

namespace util {
    class ZMQPortUtil;
    class BCCSP;
}

namespace ycsb::client {
    class NeuChainDBConnection;
}

namespace ycsb::core {
    class Status;
    class DB {
    public:
        virtual ~DB();

        virtual void stop() = 0;

        // fields: The list of fields to read, or null for all of them
        virtual core::Status read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) = 0;

        // fields: The list of fields to read, or null for all of them
        virtual core::Status scan(const std::string& table, const std::string& startKey, uint64_t recordCount, const std::vector<std::string>& fields) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status readModifyWrite(const std::string& table,
                                             const std::string& key,
                                             const std::vector<std::string>& readFields,
                                             const utils::ByteIteratorMap& writeValues) = 0;

        // values: A HashMap of field/value pairs to update in the record
        virtual core::Status insert(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) = 0;

        virtual core::Status remove(const std::string& table, const std::string& key) = 0;
    };

    class DBStatus {
    public:
        virtual ~DBStatus();

        virtual std::unique_ptr<proto::Block> getBlock(int blockNumber) = 0;

        virtual bool connect(int retryCount, int retryTimeoutMs) = 0;

        virtual bool getTop(int& blockNumber, int retryCount, int retryTimeoutMs) = 0;
    };

    class DBFactory {
    public:
        explicit DBFactory(const util::Properties &n);

        ~DBFactory();

        [[nodiscard]] std::unique_ptr<DB> newDB() const;

        [[nodiscard]] std::unique_ptr<DBStatus> newDBStatus() const;

    private:
        util::NodeConfigPtr server;
        std::unique_ptr<util::ZMQPortUtil> portConfig;
        std::shared_ptr<::ycsb::client::NeuChainDBConnection> dbc;
        std::unique_ptr<util::BCCSP> bccsp;
    };
}

