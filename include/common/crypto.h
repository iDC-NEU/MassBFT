//
// Created by peng on 11/22/22.
//

#pragma once

#include <openssl/evp.h>
#include <openssl/sha.h>
#include <memory>
#include <optional>
#include <vector>

namespace OpenSSL {
    struct Delete {
        void operator()(EVP_MD_CTX *p) const {
            EVP_MD_CTX_free(p);
        }
    };
    template<class T>
    using Ptr = std::unique_ptr<T, Delete>;
    constexpr static inline const auto SHA256 = "SHA256";
    constexpr static inline const auto SHA1 = "SHA1";

    inline void initOpenSSLCrypto() {
        OpenSSL_add_all_digests();
        OpenSSL_add_all_algorithms();
    }

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
        using EVP_MD_CTX_ptr = OpenSSL::Ptr<EVP_MD_CTX>;
    public:
        using digestType = OpenSSL::digestType<N>;
        // generic MD digest runner
        static std::optional<OpenSSL::digestType<N>> generateDigest(std::string_view data) {
            OpenSSL::digestType<N> md;
            EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new());
            if(EVP_DigestInit_ex(ctx.get(), OpenSSLHash::_digest, nullptr) &&
               EVP_DigestUpdate(ctx.get(), data.data(), data.size()) &&
               EVP_DigestFinal_ex(ctx.get(), md.data(), nullptr)) {
                return md;
            }
            return std::nullopt;
        }

        static inline auto toString(const digestType& md) {
            return OpenSSL::bytesToString<N>(md);
        }

        explicit OpenSSLHash() :ctx(EVP_MD_CTX_new()) {
            reset();
        }

        OpenSSLHash(const OpenSSLHash&) = delete;

        ~OpenSSLHash() = default;

        inline bool reset() {
            return EVP_DigestInit_ex(ctx.get(), _digest, nullptr);
        }

        inline bool update(std::string_view data) {
            return EVP_DigestUpdate(ctx.get(), data.data(), data.size());
        }

        inline std::optional<OpenSSL::digestType<N>> final() {
            OpenSSL::digestType<N> md;
            bool ret = EVP_DigestFinal_ex(ctx.get(), md.data(), &_mdLen);
            reset();
            if (!ret) {
                return std::nullopt;
            }
            return md;
        }

        inline std::optional<OpenSSL::digestType<N>> updateFinal(std::string_view data) {
            if (!update(data)) {
                return std::nullopt;
            }
            return final();
        }

    private:
        EVP_MD_CTX_ptr ctx;
        static inline const EVP_MD* _digest = EVP_MD_fetch(nullptr, mdDigestType, nullptr);
        static inline unsigned int _mdLen = EVP_MD_size(EVP_MD_fetch(nullptr, mdDigestType, nullptr));
    };

    using OpenSSLSHA256 = OpenSSLHash<OpenSSL::SHA256, 32>;
    using OpenSSLSHA1 = OpenSSLHash<OpenSSL::SHA1, 20>;
}