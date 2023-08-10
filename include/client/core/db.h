//
// Created by peng on 11/6/22.
//

#pragma once

#include "client/core/common/byte_iterator.h"
#include "proto/block.h"
#include "common/property.h"

namespace util {
    class ZMQPortUtil;
    class BCCSP;
}

namespace client {
    class NeuChainDBConnection;
}

namespace client::core {
    class Status;
    class DB {
    public:
        virtual ~DB();

        virtual void stop() = 0;

        virtual core::Status sendInvokeRequest(const std::string& chaincodeName, const std::string& funcName, const std::string& args) = 0;
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
        std::shared_ptr<::client::NeuChainDBConnection> dbc;
        std::unique_ptr<util::BCCSP> bccsp;
    };
}

