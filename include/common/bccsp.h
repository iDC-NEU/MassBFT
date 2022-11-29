//
// Created by peng on 11/28/22.
//

#pragma once
// the blockchain cryptographic service provider

#include "common/crypto.h"

#include <optional>
#include <string>
#include <memory>
#include <utility>

// Key represents a cryptographic key
class Key {
public:
    // private key version
    Key(std::string ski, std::unique_ptr<util::OpenSSLED25519> publicKey, std::unique_ptr<util::OpenSSLED25519> privateKey, bool symmetric)
        : _ski(std::move(ski)), isPrivate(true), isSymmetric(symmetric), _publicKey(std::move(*publicKey)), _privateKey(std::move(*privateKey)) {

    }

    // public key version
    Key(std::string ski, std::unique_ptr<util::OpenSSLED25519> publicKey, bool symmetric)
            : _ski(std::move(ski)), isPrivate(false), isSymmetric(symmetric), _publicKey(std::move(*publicKey)), _privateKey(nullptr) {

    }

    ~Key() = default;

    Key(const Key&) = delete;

    // Bytes converts this key to its byte representation,
    // if this operation is allowed.
    [[nodiscard]] std::optional<std::string> PrivateBytes() const {
        if (!isPrivate) {
            return std::nullopt;
        }
        return _privateKey.getHexFromPrivateKey();
    }

    [[nodiscard]] std::optional<std::string> PublicBytes() const {
        return _publicKey.getHexFromPublicKey();
    }

    // SKI returns the subject key identifier of this key.
    [[nodiscard]] std::string_view SKI() const {
        return _ski;
    }

    // Symmetric returns true if this key is a symmetric key,
    // false is this key is asymmetric
    [[nodiscard]] inline bool Symmetric() const {
        return isSymmetric;
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
    inline auto Sign(const void *d, size_t cnt) -> std::optional<util::OpenSSLED25519::digestType> {
        if (!isPrivate) {
            return std::nullopt;
        }
        return _privateKey.sign(d, cnt);
    }

    // Verify verifies signature against key k and digest
    // The opts argument should be appropriate for the algorithm used.
    // md is the signature, and d is the digest
    inline bool Verify(const util::OpenSSLED25519::digestType& md, const void *d, size_t cnt) {
        return _publicKey.verify(md, d, cnt);
    }

private:
    // unique identifier
    const std::string _ski;
    bool isPrivate;
    bool isSymmetric;
    util::OpenSSLED25519 _publicKey;
    // If private key exists, public key MUST exist.
    util::OpenSSLED25519 _privateKey;
};


// BCCSP keep ALL the keys
class BCCSP {
public:
    // KeyGen generates a key using opts.
    // Ephemeral returns true if the key to generate has to be ephemeral,
    const Key* generateED25519Key(bool ephemeral) {

    }

    // KeyDerive derives a key from k using opts.
    // The opts argument should be appropriate for the primitive used.
    const Key* KeyDerive(const Key* key, bool ephemeral) {

    }

    // KeyImport imports a key from its raw representation using opts.
    // The opts argument should be appropriate for the primitive used.
    const Key* KeyImport(const std::string& ski, const std::string& raw, bool ephemeral) {

    }

    // GetKey returns the key this CSP associates to
    // the Subject Key Identifier ski.
    const Key* GetKey(const std::string& ski) {

    }
}