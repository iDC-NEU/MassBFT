//
// Created by peng on 11/28/22.
//

#pragma once

#include "common/crypto.h"

#include "gtl/phmap.hpp"
#include <optional>
#include <string>
#include <memory>
#include <utility>

namespace util {
    // Key represents a cryptographic key
    class Key {
    public:
        // private key version
        Key(std::string ski, std::unique_ptr<util::OpenSSLED25519> publicKey,
            std::unique_ptr<util::OpenSSLED25519> privateKey, bool ephemeral)
                : _ski(std::move(ski)), isPrivate(true), isEphemeral(ephemeral), _publicKey(std::move(*publicKey)),
                  _privateKey(std::move(*privateKey)) {}

        // public key version
        Key(std::string ski, std::unique_ptr<util::OpenSSLED25519> publicKey, bool ephemeral)
                : _ski(std::move(ski)), isPrivate(false), isEphemeral(ephemeral), _publicKey(std::move(*publicKey)),
                  _privateKey(nullptr) {}

        ~Key() = default;

        Key(const Key &) = delete;

        // Bytes converts this key to its byte representation,
        // if this operation is allowed.
        [[nodiscard]] std::shared_ptr<std::string> PrivateBytes() const {
            if (!isPrivate) {
                return nullptr;
            }
            return _privateKey.getHexFromPrivateKey();
        }

        [[nodiscard]] std::shared_ptr<std::string> PublicBytes() const {
            return _publicKey.getHexFromPublicKey();
        }

        // SKI returns the subject key identifier of this key.
        [[nodiscard]] std::string_view SKI() const {
            return _ski;
        }

        [[nodiscard]] inline bool Ephemeral() const {
            return isEphemeral;
        }

        // Private returns true if this key is a private key,
        // false otherwise.
        [[nodiscard]] inline bool Private() const {
            return isPrivate;
        }

        // Sign signs digest using key k.
        // The opts argument should be appropriate for the algorithm used.
        //
        // Note that when a signature of a hash of a larger message is needed,
        // the caller is responsible for hashing the larger message and passing
        // the hash (as digest).
        inline auto Sign(const void *d, size_t cnt) const -> std::optional<util::OpenSSLED25519::digestType> {
            if (!isPrivate) {
                return std::nullopt;
            }
            return _privateKey.sign(d, cnt);
        }

        // Verify verifies signature against key k and digest
        // The opts argument should be appropriate for the algorithm used.
        // md is the signature, and d is the digest
        inline bool Verify(const util::OpenSSLED25519::digestType &md, const void *d, size_t cnt) const {
            return _publicKey.verify(md, d, cnt);
        }

    private:
        // unique identifier
        const std::string _ski;
        bool isPrivate;
        bool isEphemeral;
        util::OpenSSLED25519 _publicKey;
        // If private key exists, public key MUST exist.
        util::OpenSSLED25519 _privateKey;
    };

    using CstKeyPtr = std::shared_ptr<const Key>;
    using KeyPtr = std::shared_ptr<Key>;

    // default key storage
    class KeyStorage {
    public:
        virtual ~KeyStorage() = default;

        // Save a raw key to database, hex format
        virtual bool saveKey(std::string_view ski, std::string_view raw, bool isPrivate, bool overwrite) = 0;

        // raw key, is private
        // if exist private key, load private key
        // else, load public key
        virtual auto loadKey(std::string_view ski) -> std::optional<std::pair<std::string, bool>> = 0;
    };

    // BCCSP: the blockchain cryptographic service provider
    // BCCSP keep ALL the keys
    class BCCSP {
    public:
        explicit BCCSP(std::unique_ptr<KeyStorage> storage_) : storage(std::move(storage_)) {}

        ~BCCSP() = default;

        BCCSP(const BCCSP &) = delete;

        // KeyGen generates a key using opts.
        // Ephemeral returns true if the key to generate has to be ephemeral,
        CstKeyPtr generateED25519Key(std::string_view ski, bool ephemeral) {
            auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
            if (!ret) {
                return nullptr;
            }
            auto [pubPem, priPem] = std::move(*ret);
            auto pub = util::OpenSSLED25519::NewFromPemString(pubPem, "");
            auto pri = util::OpenSSLED25519::NewFromPemString(priPem, "");
            if (!pub || !pri) {
                return nullptr;
            }
            auto priKeyHex = pri->getHexFromPrivateKey();
            if (!priKeyHex) {
                return nullptr;
            }
            if (!ephemeral) {   // save key, we don't need to save the public key
                if (!storage->saveKey(ski, *priKeyHex, true, true)) {
                    return nullptr;
                }
            }
            // generate a key object
            auto key = std::make_shared<Key>(std::string(ski), std::move(pub), std::move(pri), ephemeral);
            cache[key->SKI()] = key;
            return key;
        }

        // KeyImport imports a key from its raw representation using opts.
        // The opts argument should be appropriate for the primitive used.
        CstKeyPtr KeyImportPEM(std::string_view ski, std::string_view raw, bool isPrivate, bool ephemeral) {
            std::unique_ptr<util::OpenSSLED25519> pri, pub;
            KeyPtr key;
            if (isPrivate) {
                // load from private key
                pri = util::OpenSSLED25519::NewFromPemString(raw, "");
                if (!pri) {
                    return nullptr;
                }
                // get raw public key
                auto ret = pri->getHexFromPublicKey();
                if (!ret) {
                    return nullptr;
                }
                // load from public key
                pub = util::OpenSSLED25519::NewPublicKeyFromHex(*ret);
                if (!pub) {
                    return nullptr;
                }
                // new key
                key = std::make_shared<Key>(std::string(ski), std::move(pub), std::move(pri), ephemeral);
                if (!ephemeral) {
                    auto ret2 = key->PrivateBytes();
                    if (!ret2) {
                        return nullptr;
                    }
                    storage->saveKey(ski, *ret2, true, true);
                }
            } else {
                // load from public key
                pub = util::OpenSSLED25519::NewFromPemString(raw, "");
                if (!pub) {
                    return nullptr;
                }
                // new key
                key = std::make_shared<Key>(std::string(ski), std::move(pub), ephemeral);
                if (!ephemeral) {
                    auto ret = key->PublicBytes();
                    if (!ret) {
                        return nullptr;
                    }
                    storage->saveKey(ski, *ret, false, true);
                }
            }
            cache[key->SKI()] = key;
            return key;
        }

        CstKeyPtr KeyImportRAW(std::string_view ski, std::string_view raw, bool isPrivate, bool ephemeral) {
            return KeyImportRAWInner<true>(ski, raw, isPrivate, ephemeral);
        }

        // GetKey returns the key this CSP associates to
        // the Subject Key Identifier ski.
        [[nodiscard]] CstKeyPtr GetKey(std::string_view ski) const {
            // load from cache
            if (cache.contains(ski)) {
                return cache.at(ski);
            }
            // load from disk
            auto ret = storage->loadKey(ski);
            if (!ret) { // we cant find it in cache or storage
                return nullptr;
            }
            auto [raw, isPrivate] = std::move(*ret);
            return KeyImportRAWInner<false>(ski, raw, isPrivate, false);
        }

    protected:
        template<bool saveToStorage>
        [[nodiscard]] CstKeyPtr KeyImportRAWInner(std::string_view ski, std::string_view raw, bool isPrivate, bool ephemeral) const {
            std::unique_ptr<util::OpenSSLED25519> pri, pub;
            KeyPtr key;
            if (isPrivate) {
                // load from private key
                pri = util::OpenSSLED25519::NewPrivateKeyFromHex(raw);
                if (!pri) {
                    return nullptr;
                }
                // get raw public key
                auto ret = pri->getHexFromPublicKey();
                if (!ret) {
                    return nullptr;
                }
                // load from public key
                pub = util::OpenSSLED25519::NewPublicKeyFromHex(*ret);
                if (!pub) {
                    return nullptr;
                }
                // new key
                key = std::make_shared<Key>(std::string(ski), std::move(pub), std::move(pri), ephemeral);
            } else {
                // load from public key
                pub = util::OpenSSLED25519::NewPublicKeyFromHex(raw);
                if (!pub) {
                    return nullptr;
                }
                // new key
                key = std::make_shared<Key>(std::string(ski), std::move(pub), ephemeral);
            }
            if (!ephemeral && saveToStorage) {
                storage->saveKey(ski, raw, isPrivate, true);
            }
            cache[key->SKI()] = key;
            return key;
        }

    private:
        mutable gtl::parallel_flat_hash_map<std::string_view, KeyPtr> cache;
        std::unique_ptr<KeyStorage> storage;
    };
}