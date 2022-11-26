//
// Created by peng on 11/22/22.
//

#pragma once

#include <openssl/evp.h>
#include <openssl/decoder.h>
#include <openssl/encoder.h>
#include <openssl/err.h>
#include <memory>
#include <optional>
#include <vector>
#include "glog/logging.h"

namespace OpenSSL {
    struct DeleteMdCtx {
        void operator()(EVP_MD_CTX *p) const {
            EVP_MD_CTX_free(p);
        }
    };
    struct DeletePkeyCtx {
        void operator()(EVP_PKEY_CTX *p) const {
            EVP_PKEY_CTX_free(p);
        }
    };
    struct DeleteDecoderCtx {
        void operator()(OSSL_DECODER_CTX *p) const {
            OSSL_DECODER_CTX_free(p);
        }
    };

    struct DeletePkey {
        void operator()(EVP_PKEY *p) const {
            EVP_PKEY_free(p);
        }
    };

    using EVP_MD_CTX_ptr = std::unique_ptr<EVP_MD_CTX, DeleteMdCtx>;
    using EVP_PKEY_CTX_ptr = std::unique_ptr<EVP_PKEY_CTX, DeletePkeyCtx>;
    using OSSL_DECODER_CTX_ptr = std::unique_ptr<OSSL_DECODER_CTX, DeleteDecoderCtx>;
    using EVP_PKEY_ptr = std::unique_ptr<EVP_PKEY, DeletePkey>;

    constexpr static inline const auto SHA256 = "SHA256";
    constexpr static inline const auto SHA1 = "SHA1";
    constexpr static inline const auto ED25519 =SN_ED25519;

    template <int N>
    using digestType = std::array<uint8_t, N>;

    template <int N>
    inline auto bytesToString(const digestType<N>& md) {
        // build output string
        static const auto hAlpha{"0123456789abcdef"};
        std::string result;
        result.reserve(md.size()*2);
        for (const auto& b : md) {
            result.push_back(hAlpha[(b >> 4) & 0xF]);
            result.push_back(hAlpha[b & 0xF]);
        }
        return result;
    }
}

namespace util {
    /// Warning:
    /// These functions are not thread-safe unless you initialize OpenSSL.
    template <auto& mdDigestType, int N>
    class OpenSSLHash {
    public:
        using digestType = OpenSSL::digestType<N>;
        // generic MD digest runner
        static std::optional<OpenSSL::digestType<N>> generateDigest(const void *d, size_t cnt) {
            OpenSSL::digestType<N> md;
            OpenSSL::EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new());
            if(EVP_DigestInit_ex(ctx.get(), OpenSSLHash::_digest, nullptr) == 1 &&
               EVP_DigestUpdate(ctx.get(), d, cnt) == 1 &&
               EVP_DigestFinal_ex(ctx.get(), md.data(), nullptr) == 1) {
                return md;
            }
            return std::nullopt;
        }

        static inline auto toString(const digestType& md) {
            return OpenSSL::bytesToString<N>(md);
        }

        static inline void initCrypto() {
            OpenSSL_add_all_digests();
            OpenSSL_add_all_algorithms();
            EVP_DigestInit_ex(ctxStatic, _digest, nullptr);
        }

        explicit OpenSSLHash() :ctx(EVP_MD_CTX_new()) {
            reset();
        }

        OpenSSLHash(const OpenSSLHash&) = delete;

        ~OpenSSLHash() = default;

        inline bool reset() {
            return EVP_MD_CTX_copy_ex(ctx.get(), ctxStatic) == 1;
        }

        inline bool update(const void *d, size_t cnt) {
            return EVP_DigestUpdate(ctx.get(), d, cnt) == 1;
        }

        inline std::optional<OpenSSL::digestType<N>> final() {
            OpenSSL::digestType<N> md;
            bool ret = EVP_DigestFinal_ex(ctx.get(), md.data(), nullptr) == 1;
            reset();
            if (!ret) {
                return std::nullopt;
            }
            return md;
        }

        inline std::optional<OpenSSL::digestType<N>> updateFinal(const void *d, size_t cnt) {
            if (!update(d, cnt)) {
                return std::nullopt;
            }
            return final();
        }

    private:
        OpenSSL::EVP_MD_CTX_ptr ctx;
        static inline EVP_MD_CTX *ctxStatic = EVP_MD_CTX_create();
        static inline const EVP_MD* _digest = EVP_MD_fetch(nullptr, mdDigestType, nullptr);
    };

    using OpenSSLSHA256 = OpenSSLHash<OpenSSL::SHA256, 32>;
    using OpenSSLSHA1 = OpenSSLHash<OpenSSL::SHA1, 20>;


    // Crypto for ECDSA and RSA
    template <auto& mdDigestType, int N>
    class OpenSSLPKCS {
    public:
        using digestType = OpenSSL::digestType<N>;

        // return publicKey, privateKey
        static auto generateKeyFiles(std::string_view pubKeyfile, std::string_view priKeyfile, std::string_view passwd) -> std::optional<std::pair<std::string, std::string>> {
            unsigned char* publicKey = nullptr;
            unsigned char* privateKey = nullptr;
            size_t publicLen = 0;
            size_t privateLen = 0;
            OpenSSL::EVP_PKEY_CTX_ptr pKeyCtx(EVP_PKEY_CTX_new_from_name(nullptr, mdDigestType, nullptr));
            if(EVP_PKEY_keygen_init(pKeyCtx.get()) != 1) {
                LOG(WARNING) << "EVP_PKEY_keygen_init fail";
                return std::nullopt;
            }

            EVP_PKEY * key = nullptr;
            // bind the pKeyCtx and fill the data
            if(EVP_PKEY_keygen(pKeyCtx.get(), &key) <= 0) {
                LOG(WARNING) << "EVP_PKEY_keygen fail fail";
                return std::nullopt;
            }

            // save to file
            auto *encoderCtx  = OSSL_ENCODER_CTX_new_for_pkey(key, EVP_PKEY_PUBLIC_KEY, "PEM", nullptr, nullptr);
            if (encoderCtx == nullptr) {
                LOG(WARNING) << "Encoder created failed";
                return std::nullopt;
            }
            if (!OSSL_ENCODER_to_data(encoderCtx, &publicKey, &publicLen)) {
                LOG(WARNING) << "EVP_PKEY_PUBLIC_KEY encode failed";
                return std::nullopt;
            }
            OSSL_ENCODER_CTX_free(encoderCtx);

            // Encode private key
            encoderCtx = OSSL_ENCODER_CTX_new_for_pkey(key, EVP_PKEY_KEYPAIR, "PEM", nullptr, nullptr);
            if (encoderCtx == nullptr) {
                LOG(WARNING) << "Encoder created failed";
                return std::nullopt;
            }
            // check if password is set
            if (!passwd.empty()) {
                OSSL_ENCODER_CTX_set_passphrase(encoderCtx, reinterpret_cast<const unsigned char *>(passwd.data()), passwd.size());
            }
            if (!OSSL_ENCODER_to_data(encoderCtx, &privateKey, &privateLen)) {
                LOG(WARNING) << "EVP_PKEY_PUBLIC_KEY encode failed";
                return std::nullopt;
            }
            OSSL_ENCODER_CTX_free(encoderCtx);

            // free the key
            EVP_PKEY_free(key);

            // print result
            // BIO_dump_fp(stdout, publicKey, (int)publicLen);
            // BIO_dump_fp(stdout, privateKey, (int)privateLen);

            // write to files
            if (!pubKeyfile.empty()) {
                BIO *out = BIO_new_file(pubKeyfile.data(), "wb");
                BIO_write(out, publicKey, (int)publicLen);
                BIO_free(out);
            }
            if (!priKeyfile.empty()) {
                BIO *out = BIO_new_file(priKeyfile.data(), "wb");
                BIO_write(out, privateKey, (int)privateLen);
                BIO_free(out);
            }
            std::string publicKeyStr(reinterpret_cast<const char *>(publicKey), publicLen);
            std::string privateKeyStr(reinterpret_cast<const char *>(privateKey), privateLen);
            OPENSSL_free(publicKey);
            OPENSSL_free(privateKey);

            return std::make_pair(publicKeyStr, privateKeyStr);
        }

        static inline auto toString(const digestType& md) {
            return OpenSSL::bytesToString<N>(md);
        }

        static std::string toHex(std::string_view readable) {
            std::string str;
            str.reserve(readable.size()/2);
            char target[3] {'\0', '\0', '\0'};
            for (std::size_t i = 0; i < readable.size(); i += 2) {
                target[0] = readable[i];
                target[1] = readable[i+1];
                str.push_back((char)std::strtol(target, nullptr, 16));
            }
            return str;
        }

        static inline void initCrypto() {
            OpenSSL_add_all_digests();
            OpenSSL_add_all_algorithms();
        }

        static std::optional<OpenSSL::digestType<N>> doSign(EVP_PKEY *pkey, const void *d, size_t cnt) {
            OpenSSL::digestType<N> md;
            size_t sigLen = md.size();
            OpenSSL::EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new());

            if (EVP_DigestSignInit_ex(ctx.get(), nullptr, nullptr, nullptr, nullptr, pkey, nullptr) != 1) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return std::nullopt;
            }
            if (EVP_DigestSign(ctx.get(), md.data(), &sigLen, reinterpret_cast<const unsigned char *>(d), cnt) != 1) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return std::nullopt;
            }
            return md;
        }

        static bool doVerify(EVP_PKEY *pkey, const OpenSSL::digestType<N>& md, const void *d, size_t cnt) {
            OpenSSL::EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new());

            if (EVP_DigestVerifyInit_ex(ctx.get(), nullptr, nullptr, nullptr, nullptr, pkey, nullptr) != 1) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return false;
            }
            if(EVP_DigestVerify(ctx.get(), md.data(), md.size(), reinterpret_cast<const unsigned char *>(d), cnt) != 1) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return false;
            }
            return true;
        }

    protected:
        static EVP_PKEY* decodePEM(std::string_view pemString, std::string_view password) {
            // the actual private key
            EVP_PKEY *pkey = nullptr;
            // a buffer context
            OpenSSL::OSSL_DECODER_CTX_ptr dCtx(OSSL_DECODER_CTX_new_for_pkey(&pkey, nullptr, nullptr, nullptr, 0, nullptr, nullptr));
            if (dCtx == nullptr) {
                LOG(WARNING) << "OSSL_DECODER_CTX_new_for_pkey decode failed";
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return nullptr;
            }
            if (!password.empty()) {
                OSSL_DECODER_CTX_set_passphrase(dCtx.get(), reinterpret_cast<const unsigned char *>(password.data()), password.size());
            }

            auto data = reinterpret_cast<const unsigned char *>(pemString.data());
            size_t dataLen = pemString.size();

            // fill in pkey
            if (OSSL_DECODER_from_data(dCtx.get(), &data, &dataLen) != 1) {
                LOG(WARNING) << "OSSL_DECODER_from_data decode failed";
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return nullptr;
            }
            return pkey;
        }

        static std::optional<std::string> loadPemFile(std::string_view pemPath) {
            BIO *bp = BIO_new(BIO_s_file());;
            auto ret = BIO_read_filename(bp, pemPath.data());
            if (ret != 1) {
                return std::nullopt;
            }

            std::string buf;
            buf.resize(1024*1024);    // 1MB is enough for a pem file
            auto len = BIO_read(bp, buf.data(), (int)buf.size());
            BIO_free(bp);
            if (len <= 0) {
                LOG(WARNING) << "Read key from file failed";
                return std::nullopt;
            }
            buf.resize(len);
            return buf;
        }

    public:
        explicit OpenSSLPKCS(EVP_PKEY* pkey_) :pkey(pkey_) { }

        static std::unique_ptr<OpenSSLPKCS> NewFromPemString(std::string_view pemString, std::string_view password) {
            auto key = OpenSSLPKCS::decodePEM(pemString, password);
            if (key == nullptr) {
                return nullptr;
            }
            return std::make_unique<OpenSSLPKCS>(key);
        }

        static std::unique_ptr<OpenSSLPKCS> NewPrivateKeyFromHex(std::string_view hex) {
            auto key = EVP_PKEY_new_raw_private_key_ex(nullptr, mdDigestType, nullptr, reinterpret_cast<const unsigned char *>(hex.data()), hex.size());
            if (key == nullptr) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return nullptr;
            }
            return std::make_unique<OpenSSLPKCS>(key);
        }

        static std::unique_ptr<OpenSSLPKCS> NewPublicKeyFromHex(std::string_view hex) {
            auto key = EVP_PKEY_new_raw_public_key_ex(nullptr, mdDigestType, nullptr, reinterpret_cast<const unsigned char *>(hex.data()), hex.size());
            if (key == nullptr) {
                LOG(WARNING) << ERR_error_string(ERR_get_error(), nullptr);
                return nullptr;
            }
            return std::make_unique<OpenSSLPKCS>(key);
        }

        static std::unique_ptr<OpenSSLPKCS> NewFromPemFile(std::string_view pemPath, std::string_view password) {
            auto ret = OpenSSLPKCS::loadPemFile(pemPath);
            if(!ret) {
                return nullptr;
            }
            return NewFromPemString(*ret, password);
        }

        OpenSSLPKCS(const OpenSSLPKCS&) = delete;

        ~OpenSSLPKCS() = default;

        inline std::optional<OpenSSL::digestType<N>> sign(const void *d, size_t cnt) const {
            return OpenSSLPKCS::doSign(pkey.get(), d, cnt);
        }

        inline bool verify(const OpenSSL::digestType<N>& md, const void *d, size_t cnt) const {
            return OpenSSLPKCS::doVerify(pkey.get(), md, d, cnt);
        }

    private:
        OpenSSL::EVP_PKEY_ptr pkey;
    };

    using OpenSSLED25519 = OpenSSLPKCS<OpenSSL::ED25519, 64>;

}