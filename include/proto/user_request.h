//
// Created by peng on 2/21/23.
//

#pragma once

#include "common/crypto.h"
#include "zpp_bits.h"

namespace proto {
    using DigestString = util::OpenSSLED25519::digestType;

    // publicKeyHex, signature
    struct SignatureString {
    public:
        std::string ski;
        DigestString digest{};

        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, SignatureString &s) {
            return archive(s.ski, s.digest);
        }
    };

    class UserRequest {
    public:
        UserRequest() = default;

        UserRequest(const UserRequest &rhs) = delete;

        UserRequest(UserRequest &&rhs) = delete;

        virtual ~UserRequest() = default;

        void setCCName(std::string &&ccName) {
            _ccName = std::move(ccName);
            _ccNameSV = _ccName;
        }

        [[nodiscard]] const std::string_view &getCCNameSV() const {
            return _ccNameSV;
        }

        void setFuncName(std::string &&funcName) {
            _funcName = std::move(funcName);
            _funcNameSV = _funcName;
        }

        [[nodiscard]] const std::string_view &getFuncNameSV() const {
            return _funcNameSV;
        }

        // args has type std::string
        void setArgs(auto &&args) {
            _args = std::forward<decltype(args)>(args);
            _argsSV = _args;
        }

        void setNonce(auto &&nonce) {
            _nonce = std::forward<decltype(nonce)>(nonce);
        }

        [[nodiscard]] const std::string_view &getArgs() const { return _argsSV; }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, auto &t) {
            return archive(t._ccNameSV, t._funcNameSV, t._argsSV, t._nonce);
        }

    private:
        std::string _ccName;
        std::string_view _ccNameSV;
        std::string _funcName;
        std::string_view _funcNameSV;
        std::string _args;
        std::string_view _argsSV;
        int64_t _nonce{};
    };

    class DeserializeStorage {
    public:
        DeserializeStorage() = default;

        virtual ~DeserializeStorage() = default;

        DeserializeStorage(const DeserializeStorage &rhs) {
            storage = rhs.storage;
        }

        DeserializeStorage(DeserializeStorage &&rhs) noexcept {
            storage = std::move(rhs.storage);
        }

        void setSerializedMessage(std::string &&m) {
            storage = std::make_shared<std::string>(std::move(m));
        }

        void setSerializedMessage(std::unique_ptr<std::string> m) {
            storage = std::make_shared<std::string>(std::move(*m));
        }

        std::shared_ptr<std::string> getSerializedMessage() {
            return storage;
        }

        [[nodiscard]] std::shared_ptr<const std::string> getSerializedMessage() const {
            return storage;
        }

        [[nodiscard]] bool haveSerializedMessage() const {
            return storage != nullptr && !storage->empty();
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &, DeserializeStorage &) {
            return zpp::bits::errc{};
        }

    protected:
        std::shared_ptr<std::string> storage;
    };

    class Envelop {
    public:
        Envelop() = default;

        Envelop(const Envelop &rhs) = delete;

        Envelop(Envelop &&rhs) = delete;

        void setPayload(std::string &&raw) {
            _payload = std::move(raw);
            _payloadSV = _payload;
        }

        [[nodiscard]] const std::string_view &getPayload() const { return _payloadSV; }

        template<class T>
        requires requires(T t) { SignatureString(t); }
        void setSignature(T t) {
            _signature = std::forward<T>(t);
        }

        [[nodiscard]] const SignatureString& getSignature() const {
            return _signature;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, auto &e) {
            return archive(e._payloadSV, e._signature);
        }

        int deserializeFromString(std::string_view buf, int pos = 0) {
            auto in = zpp::bits::in(buf);
            in.reset(pos);
            // we must set the actual payload, since the outside message may be empty
            if(failure(in(_payload, _signature))) {
                return -1;
            }
            _payloadSV = _payload;
            return (int)in.position();
        }

        bool serializeToString(std::string *buf, int pos = 0) const {
            zpp::bits::out out(*buf);
            out.reset(pos);
            if(failure(out(*this))) {
                return false;
            }
            return true;
        }

    private:
        std::string_view _payloadSV;
        std::string _payload;
        SignatureString _signature;
    };
}