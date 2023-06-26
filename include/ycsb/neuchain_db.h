//
// Created by peng on 11/6/22.
//

#pragma once

#include "ycsb/core/db.h"
#include "ycsb/core/status.h"
#include "proto/block.h"
#include "common/zeromq.h"

namespace ycsb::client {
    class NeuChainDB: public ycsb::core::DB {
    public:
        NeuChainDB();

        core::Status read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) override;

        core::Status scan(const std::string& table, const std::string& startKey, uint64_t recordCount, const std::vector<std::string>& fields) override;

        core::Status update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) override;

        core::Status readModifyWrite(const std::string& table,
                                     const std::string& key,
                                     const std::vector<std::string>& readFields,
                                     const utils::ByteIteratorMap& writeValues) override;

        core::Status insert(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) override;

        core::Status remove(const std::string& table, const std::string& key) override;

    protected:
        bool sendInvokeRequest(const proto::UserRequest &request);

    private:
        enum class Op { READ, WRITE };
        std::unique_ptr<util::ZMQInstance> invokeClient;
    };


    class NeuChainStatus: public ycsb::core::DBStatus {
    public:
        NeuChainStatus();

        std::unique_ptr<proto::Block> getBlock(int blockNumber) override;

    private:
        std::unique_ptr<util::ZMQInstance> rpcClient;
    };
}
