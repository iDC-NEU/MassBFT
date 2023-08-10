//
// Created by peng on 11/6/22.
//

#pragma once

#include "client/core/db.h"
#include "client/core/status.h"
#include "proto/block.h"
#include "common/bccsp.h"

namespace brpc {
    class Channel;
}

namespace client {
    namespace proto {
        class UserService_Stub;
    }

    struct UserRequestLikeStruct {
        std::string_view _ccNameSV;
        std::string_view _funcNameSV;
        std::string_view _argsSV;
    };

    struct EnvelopLikeStruct {
        std::string_view _payloadSV;
        ::proto::SignatureString _signature;
    };

    class NeuChainDBConnection;

    class NeuChainDB: public core::DB {
    public:
        NeuChainDB(util::NodeConfigPtr server, std::shared_ptr<NeuChainDBConnection> dbc, std::shared_ptr<const util::Key> priKey);

        void stop() override;

    protected:
        core::Status sendInvokeRequest(const std::string& chaincodeName, const std::string& funcName, const std::string& args) override;

    private:
        int64_t _nextNonce;
        std::shared_ptr<NeuChainDBConnection> _invokeClient;
        std::shared_ptr<const util::Key> _priKey;
        util::NodeConfigPtr _serverConfig;
    };

    class NeuChainStatus: public core::DBStatus {
    public:
        NeuChainStatus(util::NodeConfigPtr server, int port, std::shared_ptr<const util::Key> priKey);

        ~NeuChainStatus() override;

        std::unique_ptr<::proto::Block> getBlock(int blockNumber) override;

        bool connect(int retryCount, int retryTimeoutMs) override;

        bool getTop(int& blockNumber, int retryCount, int retryTimeoutMs) override;

    private:
        std::unique_ptr<brpc::Channel> _channel;
        std::unique_ptr<proto::UserService_Stub> _stub;
        std::shared_ptr<const util::Key> _priKey;
        util::NodeConfigPtr _serverConfig;
    };
}
