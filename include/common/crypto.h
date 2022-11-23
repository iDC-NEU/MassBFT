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
}

namespace util {
    /// Warning:
    /// These functions are not thread-safe unless you initialize OpenSSL.
    class OpenSSLHash {
    public:
        using digestType = std::vector<uint8_t>;
    protected:
        using EVP_MD_CTX_ptr = OpenSSL::Ptr<EVP_MD_CTX>;
        // generic MD digest runner
        static std::optional<digestType> generateDigest(std::string_view data, const EVP_MD* digest) {
            unsigned int mdLen = EVP_MD_size(digest);
            digestType md(mdLen);
            EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new());
            if(EVP_DigestInit_ex(ctx.get(), digest, nullptr) &&
               EVP_DigestUpdate(ctx.get(), data.data(), data.size()) &&
               EVP_DigestFinal_ex(ctx.get(), md.data(), &mdLen)) {
                return md;
            }
            return std::nullopt;
        }
    public:
        static void initOpenSSLCrypto() {
            OpenSSL_add_all_digests();
            OpenSSL_add_all_algorithms();
        }

        static inline auto generateSHA1(std::string_view data) {
            return generateDigest(data, EVP_sha1());
        }

        static inline auto generateSHA256(std::string_view data) {
            return generateDigest(data, EVP_sha256());
        }

        static inline auto bytesToString(const digestType& md) {
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

        explicit OpenSSLHash(const EVP_MD* digest=EVP_sha256()) :ctx(EVP_MD_CTX_new()), _digest(digest), _mdLen(EVP_MD_size(digest)) {
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

        inline std::optional<digestType> final() {
            digestType md(_mdLen);
            bool ret = EVP_DigestFinal_ex(ctx.get(), md.data(), &_mdLen);
            reset();
            if (!ret) {
                return std::nullopt;
            }
            return md;
        }

        inline std::optional<digestType> updateFinal(std::string_view data) {
            if (!update(data)) {
                return std::nullopt;
            }
            return final();
        }

    private:
        EVP_MD_CTX_ptr ctx;
        const EVP_MD* _digest;
        unsigned int _mdLen;
    };
}