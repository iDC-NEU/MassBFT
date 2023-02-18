//
// Created by peng on 12/5/22.
//

#pragma once

#include "common/crypto.h"
#include "common/parallel_merkle_tree.h"
#include "zpp_bits.h"

namespace proto {
    using HashString = util::OpenSSLSHA256::digestType;
    // publicKeyHex, signature
    struct SignatureString {
        std::string ski;
        std::shared_ptr<std::string> pubKey;    // optional
        util::OpenSSLED25519::digestType digest{};
    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, SignatureString &s) {
            return archive(s.ski, s.pubKey, s.digest);
        }
    };

    using BlockNumber = uint64_t;

    class DeserializeStorage {
    public:
        DeserializeStorage() = default;

        DeserializeStorage(const DeserializeStorage &rhs) {
            storage = rhs.storage;
        }

        DeserializeStorage(DeserializeStorage &&rhs) noexcept {
            storage = std::move(rhs.storage);
        }

        void setSerializedMessage(std::string &&m) {
            storage = std::make_shared<std::string>(std::move(m));
        }

        void setSerializedMessage(std::string *m) {
            storage = std::make_shared<std::string>(std::move(*m));
        }

        std::shared_ptr<const std::string> getSerializedMessage() {
            return storage;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &, DeserializeStorage &) {
            return zpp::bits::errc{};
        }

    protected:
        std::shared_ptr<const std::string> storage;
    };

    // When deserialized, the caller need to ensure the active data is still valid.
    class KV {
    public:
        template<class T1, class T2>
        requires requires(T1 t1, T2 t2) { std::string(t1); std::string(t2); }
        KV(T1 &&key, T2 &&value)
                : _key(std::forward<T1>(key)), _value(std::forward<T2>(value)),
                  _keySV(_key), _valueSV(_value) {}

        KV() = default;

        KV(const KV &rhs) = delete;

        KV(KV &&rhs) = delete;

        void setKey(std::string &&key) {
            _key = std::move(key);
            _keySV = _key;
        }

        void setValue(std::string &&value) {
            _value = std::move(value);
            _valueSV = _value;
        }

        [[nodiscard]] const std::string_view &getKeySV() const {
            return _keySV;
        }

        [[nodiscard]] const std::string_view &getValueSV() const {
            return _valueSV;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, KV &kv) {
            return archive(kv._keySV, kv._valueSV);
        }

    private:
        std::string _key;
        std::string _value;
        std::string_view _keySV;
        std::string_view _valueSV;
    };

    class TxReadWriteSet {
    public:
        template<class T>
        requires requires(T t) { std::string(t); }
        explicit TxReadWriteSet(T &&ccName, int32_t retCode=0)
                : _ccName(std::forward<T>(ccName)), _ccNameSV(_ccName), _retCode(retCode) {}

        TxReadWriteSet() : _ccName(), _ccNameSV(_ccName), _retCode(0) {}

        TxReadWriteSet(const TxReadWriteSet &rhs) = delete;

        TxReadWriteSet(TxReadWriteSet &&rhs) = delete;

        void setCCName(std::string &&ccName) {
            _ccName = std::move(ccName);
            _ccNameSV = _ccName;
        }

        [[nodiscard]] const std::string_view &getCCNameSV() const {
            return _ccNameSV;
        }

        void setRetCode(int32_t retCode) {
            _retCode = retCode;
        }

        [[nodiscard]] int32_t getRetCode() const {
            return _retCode;
        }

        [[nodiscard]] const auto &getReads() const {
            return _reads;
        }

        [[nodiscard]] const auto &getWrites() const {
            return _writes;
        }

        [[nodiscard]] auto &getReads() {
            return _reads;
        }

        [[nodiscard]] auto &getWrites() {
            return _writes;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, TxReadWriteSet &t) {
            return archive(t._ccNameSV, t._reads, t._writes);
        }

    private:
        std::string _ccName;
        std::string_view _ccNameSV;
        int32_t _retCode;
        std::vector<std::unique_ptr<KV>> _reads;
        std::vector<std::unique_ptr<KV>> _writes;
    };

    class UserRequest {
    public:
        UserRequest() : _ccName(), _ccNameSV(_ccName) {}

        UserRequest(const UserRequest &rhs) = delete;

        UserRequest(UserRequest &&rhs) = delete;

        void setCCName(std::string &&ccName) {
            _ccName = std::move(ccName);
            _ccNameSV = _ccName;
        }

        [[nodiscard]] const std::string_view &getCCNameSV() const {
            return _ccNameSV;
        }

        void setArgs(std::vector<std::string> &&args) {
            _args = std::move(args);
            _argsSV.clear();
            _argsSV.reserve(_args.size());
            for (const auto &it: _args) {
                _argsSV.push_back(it);
            }
        }

        [[nodiscard]] const std::vector<std::string_view> &getArgs() const { return _argsSV; }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, UserRequest &t) {
            return archive(t._ccNameSV, t._args);
        }

    private:
        std::string _ccName;
        std::string_view _ccNameSV;
        std::vector<std::string> _args;
        std::vector<std::string_view> _argsSV;
    };

    // WARNING: ENVELOP HAS STRING_VIEW TYPE, MUST CONSIDER DANGING POINTER
    class Envelop : public DeserializeStorage {
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

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, Envelop &e) {
            return archive(e._payloadSV, e._signature);
        }

    private:
        std::string_view _payloadSV;
        std::string _payload;
        SignatureString _signature;
    };

    class Block : public DeserializeStorage {
    public:
        class Header {
        public:
            BlockNumber number{};
            // previous hash of ALL the user request
            HashString previousHash{};
            // current user request hash
            HashString dataHash{};

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Header &h) {
                return archive(h.number, h.previousHash, h.dataHash);
            }
        };

        // a vector of transaction
        class Body {
        public:
            // userRequests: represent the raw request sent by the user
            std::vector<std::unique_ptr<Envelop>> userRequests;

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Body &b) {
                return archive(b.userRequests);
            }
        };

        class ExecuteResult {
        public:
            std::vector<std::unique_ptr<TxReadWriteSet>> txReadWriteSet;
            // check if a transaction is valid
            std::vector<std::byte> transactionFilter;

        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, ExecuteResult &e) {
                return archive(e.txReadWriteSet, e.transactionFilter);
            }
        };

        class Metadata {
        public:
            // when a peer validated a block(before execution), it adds its signature of the HEADER+BODY to signatures.
            std::vector<SignatureString> consensusSignatures;
            // when a peer validated a block(after execution), it adds its signature of the (HEADER+BODY+ExecuteResult) to signatures.
            std::vector<SignatureString> validateSignatures;
        public:
            friend zpp::bits::access;

            constexpr static auto serialize(auto &archive, Metadata &m) {
                return archive(m.consensusSignatures, m.validateSignatures);
            }
        };

    public:
        Block() = default;

        Block(const Block &) = delete;

        Block(Block &&) = delete;

        Header header;
        Body body;
        ExecuteResult executeResult;
        Metadata metadata;

        struct PosList {
            bool valid = false;
            std::size_t headerPos{};
            std::size_t bodyPos{};
            std::size_t execResultPos{};
            std::size_t metadataPos{};
            std::size_t endPos{};
        };

        PosList deserializeFromString(std::string &&raw, int pos = 0) {
            PosList posList;
            this->setSerializedMessage(std::move(raw));
            auto in = zpp::bits::in(*(this->storage));
            in.reset(pos);
            posList.headerPos = in.position();
            if(failure(in(this->header))) {
                return posList;
            }
            posList.bodyPos = in.position();
            if(failure(in(this->body))) {
                return posList;
            }
            posList.execResultPos = in.position();
            if(failure(in(this->executeResult))) {
                return posList;
            }
            posList.metadataPos = in.position();
            if(failure(in(this->metadata))) {
                return posList;
            }
            posList.endPos = in.position();
            posList.valid = true;
            return posList;
        }

        // all pointer must be not null!
        bool serializeToString(std::string *buf, int pos = 0) {
            zpp::bits::out out(*buf);
            out.reset(pos);
            if(failure(out(*this))) {
                return false;
            }
            return true;
        }
    };
}
