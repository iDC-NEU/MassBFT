//
// Created by user on 23-8-9.
//

#include <client/smallbank_db.h>

#include "common/timer.h"
#include "glog/logging.h"
#include "proto/user_request.h"
#include "client/core/common/random_uint64.h"

namespace brpc::policy {
    DECLARE_int32(h2_client_connection_window_size);
}

namespace client {
    SmallBankDB::SmallBankDB(util::NodeConfigPtr server, std::shared_ptr<NeuChainDBConnection> dbc,
                             std::shared_ptr<const util::Key> priKey) {
        if (brpc::policy::FLAGS_h2_client_connection_window_size < 1024 * 1024 * 10) {
            brpc::policy::FLAGS_h2_client_connection_window_size = 1024 * 1024 * 10;
        }
        _nextNonce = static_cast<int64_t>(utils::RandomUINT64::NewRandomUINT64()->nextValue() << 32);
        LOG(INFO) << "Created a connection to SmallBankDB with nonce: " << _nextNonce;
        CHECK(priKey->Private()) << "Can not sign using public key!";
        _invokeClient = std::move(dbc);
        _priKey = std::move(priKey);
        _serverConfig = std::move(server);
    }

    void SmallBankDB::stop() {
        _invokeClient->shutdown();
    }

    core::Status SmallBankDB::amalgamate(uint32_t acc1, uint32_t acc2) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc1, acc2))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::AMALGAMATE, data);
    }

    core::Status SmallBankDB::getBalance(uint32_t acc) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::GET_BALANCE, data);
    }

    core::Status SmallBankDB::updateBalance(uint32_t acc, uint32_t amount) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::UPDATE_BALANCE, data);
    }

    core::Status SmallBankDB::updateSaving(uint32_t acc, uint32_t amount) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::UPDATE_SAVING, data);
    }

    core::Status SmallBankDB::sendPayment(uint32_t acc1, uint32_t acc2, unsigned int amount) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc1, acc2, amount))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::SEND_PAYMENT, data);
    }

    core::Status SmallBankDB::writeCheck(uint32_t acc, uint32_t amount) {
        std::string data;
        zpp::bits::out out(data);
        if (failure(out(acc, amount))) {
            return core::ERROR;
        }
        return sendInvokeRequest(InvokeRequestType::WRITE_CHECK, data);
    }

    core::Status SmallBankDB::sendInvokeRequest(const std::string &funcName,
                                                const std::string &args) {
        // TODO: copy from neuchain_db
        // archive manually
        std::string data;
        zpp::bits::out out(data);
        UserRequestLikeStruct u{
                InvokeRequestType::SMALL_BANK,
                funcName,
                args
        };
        if (failure(::proto::UserRequest::serialize(out, u))) {
            return core::ERROR;
        }
        EnvelopLikeStruct e;
        // append the nonce
        e._signature.nonce = _nextNonce;
        if (failure(out(e._signature.nonce))) {
            return core::ERROR;
        }
        _nextNonce += 1;
        // sign the envelope
        auto ret = _priKey->Sign(data.data(), data.size());
        if (ret == std::nullopt) {
            return core::ERROR;
        }
        e._payloadSV = data;
        e._signature.digest = *ret;
        e._signature.ski = _serverConfig->ski;
        // serialize the data
        std::string dataEnvelop;
        zpp::bits::out outEnvelop(dataEnvelop);
        if (failure(::proto::Envelop::serialize(outEnvelop, e))) {
            return core::ERROR;
        }
        auto timeNowMs = util::Timer::time_now_ms();
        if (!_invokeClient->send(std::move(dataEnvelop))) {
            return core::ERROR;
        }
        return core::Status(core::Status::State::OK,
                            timeNowMs,
                            std::string(reinterpret_cast<const char *>(e._signature.digest.data()),
                                        e._signature.digest.size()));
    }
}
