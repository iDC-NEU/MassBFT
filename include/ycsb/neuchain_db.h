//
// Created by peng on 11/6/22.
//

#ifndef NEUCHAIN_PLUS_NEUCHAIN_DB_H
#define NEUCHAIN_PLUS_NEUCHAIN_DB_H

#include "ycsb/core/db.h"
#include "ycsb/core/status.h"
#include "proto/block.h"
#include "common/zeromq.h"
#include "proto/user_request.h"

namespace ycsb::client {
    class NeuChainDB: public ycsb::core::DB {

    public:
        NeuChainDB() {
            db_connection();
            LOG(INFO) << "Created a connection to NeuChainDB.";
        }

        core::Status db_connection() override;

        core::Status read(const std::string& table, const std::string& key, const std::vector<std::string>& fields) override;

        core::Status scan(const std::string& table, const std::string& startkey, uint64_t recordcount, const std::vector<std::string>& fields) override;

        core::Status update(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) override;

        core::Status readModifyWrite(const std::string& table,
                                     const std::string& key,
                                     const std::vector<std::string>& readFields,
                                     const utils::ByteIteratorMap& writeValues) override;

        core::Status insert(const std::string& table, const std::string& key, const utils::ByteIteratorMap& values) override;

        core::Status remove(const std::string& table, const std::string& key) override;

        std::unique_ptr<proto::Block> getBlock(int blockNumber) override;

    protected:
        std::function<void(proto::UserRequest &request)> sendInvokeRequest;

    private:
        std::vector<std::pair<std::string, std::unique_ptr<util::ZMQInstance>>> rpcClient;
        std::vector<std::pair<std::string, std::unique_ptr<util::ZMQInstance>>> invokeClient;
        size_t trCount{};
    };
}

#endif //NEUCHAIN_PLUS_NEUCHAIN_DB_H
