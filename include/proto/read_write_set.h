//
// Created by peng on 2/21/23.
//

#pragma once

#include "common/crypto.h"
#include "zpp_bits.h"

namespace proto {
    using DigestString = util::OpenSSLED25519::digestType;
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

        [[nodiscard]] bool equals(const KV& rhs) const {
            return this->_keySV == rhs._keySV && this->_valueSV == rhs._valueSV;
        }

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

    using KVList = std::vector<std::unique_ptr<KV>>;

    class TxReadWriteSet {
    public:
        explicit TxReadWriteSet(DigestString requestDigest)
                : _requestDigest(requestDigest), _retCode(-1) {}

        void setRequestDigest(DigestString requestDigest) {
            _requestDigest = requestDigest;
        }

        TxReadWriteSet() : TxReadWriteSet(DigestString{}) {}

        TxReadWriteSet(const TxReadWriteSet &rhs) = delete;

        TxReadWriteSet(TxReadWriteSet &&rhs) = delete;

        virtual ~TxReadWriteSet() = default;

        [[nodiscard]] const DigestString& getRequestDigest() const {
            return _requestDigest;
        }

        void setCCNamespace(std::string &&ccNamespace) {
            _ccNamespace = std::move(ccNamespace);
            _ccNamespaceSV = _ccNamespace;
        }

        [[nodiscard]] const std::string_view &getCCNamespaceSV() const {
            return _ccNamespaceSV;
        }

        [[nodiscard]] const KVList &getReads() const {
            return _reads;
        }

        [[nodiscard]] const KVList &getWrites() const {
            return _writes;
        }

        [[nodiscard]] KVList &getReads() {
            return _reads;
        }

        [[nodiscard]] KVList &getWrites() {
            return _writes;
        }
        void setRetCode(int32_t retCode) {
            _retCode = retCode;
        }

        [[nodiscard]] int32_t getRetCode() const {
            return _retCode;
        }

    public:
        friend zpp::bits::access;

        constexpr static auto serialize(auto &archive, TxReadWriteSet &t) {
            return archive(t._requestDigest, t._ccNamespaceSV, t._retCode, t._reads, t._writes);
        }

    private:
        // requestDigest(tid) represents the corresponding user request
        DigestString _requestDigest;
        std::string _ccNamespace;
        std::string_view _ccNamespaceSV;
        int32_t _retCode;
        KVList _reads;
        KVList _writes;
    };

}