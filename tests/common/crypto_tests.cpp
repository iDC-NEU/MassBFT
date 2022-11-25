//
// Created by peng on 11/22/22.
//

#include "common/crypto.h"
#include "common/timer.h"

#include "gtest/gtest.h"
#include "glog/logging.h"
#include <vector>


class CryptoTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
    constexpr static const auto msg = "The quick brown fox jumps over the lazy dog";
    constexpr static const auto kat1 = "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12";
    constexpr static const auto kat256 = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592";
};

TEST_F(CryptoTest, TestStaticFunc) {
    util::OpenSSLSHA1::initCrypto();
    util::OpenSSLSHA256::initCrypto();
    util::OpenSSLSHA1::digestType defaultSha1Msg;
    util::OpenSSLSHA256::digestType defaultSha256Msg;
    auto msgView = std::string_view(msg);
    ASSERT_TRUE(util::OpenSSLSHA1::toString(util::OpenSSLSHA1::generateDigest(msgView.data(), msgView.size()).value_or(defaultSha1Msg)) == kat1) << "SHA1 FAIL";
    ASSERT_TRUE(util::OpenSSLSHA256::toString(util::OpenSSLSHA256::generateDigest(msgView.data(), msgView.size()).value_or(defaultSha256Msg)) == kat256) << "SHA256 FAIL";
}

TEST_F(CryptoTest, TestDynamicFunc) {
    util::OpenSSLSHA256::initCrypto();
    util::OpenSSLSHA256::digestType defaultSha256Msg;
    auto hash = util::OpenSSLSHA256();
    auto msgView = std::string_view(msg);
    ASSERT_TRUE(hash.update(msgView.data(), msgView.size())) << "update failed";
    ASSERT_TRUE(util::OpenSSLSHA256::toString(hash.final().value_or(defaultSha256Msg)) == kat256) << "SHA256 FAIL";

    ASSERT_TRUE(hash.update(msgView.data(), 10)) << "update failed";
    ASSERT_TRUE(util::OpenSSLSHA256::toString(hash.updateFinal(msgView.data()+10, msgView.size()-10).value_or(defaultSha256Msg)) == kat256) << "SHA256 FAIL";
}

TEST_F(CryptoTest, SingleThreadPerformance) {
    util::OpenSSLSHA256::initCrypto();
    auto hash = util::OpenSSLSHA256();
    std::string dataEncode; // each thread use a different data
    for (int i=0; i<200000; i++) {
        dataEncode += std::to_string(i*3);
    }
    LOG(INFO) << "Data Size: " << dataEncode.size();
    util::Timer timer;
    for (int i=0; i<1000; i++) {
        hash.updateFinal(dataEncode.data(), dataEncode.size());
    }
    LOG(INFO) << "Total Time spend: " << timer.end();
}



TEST_F(CryptoTest, TestED) {


    std::string key_ {
            R"(
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEDd9VXmpHjV4voFO+0ZoFPlRr5icZXquxsr9EkaOUO9B7Wl1DAgGI0EKCm1++Bl2Od32xZFeFuG07OTpTMVOCPA==
-----END PUBLIC KEY-----
)"
    };
    std::string msg { util::OpenSSLED25519::toHex("562d6ddfb3ceb5abb12d97bc35c4963d249f55b7c75eda618d365492ee98d469") };
    std::string sig { util::OpenSSLED25519::toHex("304502204d6d070117d445f4c2fcdbd4df037a1c8cfee2a166353c2e562cd5efd06e914d022100bb06439ded1478bd19022519dc06a84ba18ea4bf30ea9eb9ea90f8b66dad12c7") };

    util::OpenSSLED25519::initCrypto();
    auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
    if(!ret) {
        return;
    }
    auto [pub, pri] = std::move(*ret);

    auto signer = util::OpenSSLED25519::NewFromPemString(pri, true, {});
    auto validator = util::OpenSSLED25519::NewFromPemString(pub, false, {});

    auto data = std::string_view("Test sign data: 304502204d6d070117d445f4c2fcdbd4df037a1c8cfee2a166353c2e562cd5efd06e914d022100bb06439ded1478bd19022519dc06a84ba18ea4bf30ea9eb9ea90f8b66dad12c7");

    auto signatureRet = signer->sign(data.data(), data.size());
    if (!signatureRet) {
        return;
    }
    auto signature = *signatureRet;
    LOG(INFO) << validator->verify(signature, data.data(), data.size());


}