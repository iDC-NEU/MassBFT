//
// Created by user on 23-8-9.
//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "client/core/status.h"
#include "common/bccsp.h"
#include "common/property.h"
#include "neuchain_dbc.h"
#include "proto/block.h"

namespace client {
    struct UserRequestLikeStruct {
        std::string_view _ccNameSV;
        std::string_view _funcNameSV;
        std::string_view _argsSV;
    };

    struct EnvelopLikeStruct {
        std::string_view _payloadSV;
        ::proto::SignatureString _signature;
    };

    class SmallBankDB {
    public:
        struct InvokeRequestType {
            constexpr static const auto SMALL_BANK = "smallBank";
            constexpr static const auto AMALGAMATE = "amalgamate";
            constexpr static const auto GET_BALANCE = "getBalance";
            constexpr static const auto UPDATE_BALANCE = "updateBalance";
            constexpr static const auto UPDATE_SAVING = "updateSaving";
            constexpr static const auto SEND_PAYMENT = "sendPayment";
            constexpr static const auto WRITE_CHECK = "writeCheck";
        };

        SmallBankDB(util::NodeConfigPtr server, std::shared_ptr<NeuChainDBConnection> dbc,
                    std::shared_ptr<const util::Key> priKey);

        void stop();

        core::Status amalgamate(uint32_t acc1, uint32_t acc2);

        core::Status getBalance(uint32_t acc);

        core::Status updateBalance(uint32_t acc, uint32_t amount);

        core::Status updateSaving(uint32_t acc, uint32_t amount);

        core::Status sendPayment(uint32_t acc1, uint32_t acc2, unsigned amount);

        core::Status writeCheck(uint32_t acc, uint32_t amount);

    protected:
        core::Status sendInvokeRequest(const std::string &funcName, const std::string &args);

    private:
        int64_t _nextNonce;
        std::shared_ptr<const util::Key> _priKey;
        util::NodeConfigPtr _serverConfig;
        std::shared_ptr<NeuChainDBConnection> _invokeClient;
    };
}