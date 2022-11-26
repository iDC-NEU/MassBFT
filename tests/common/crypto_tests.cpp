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

TEST_F(CryptoTest, TestED25519KeyPairGeneration) {
    util::OpenSSLED25519::initCrypto();
    auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
    if(!ret) {
        return;
    }
    auto [pub, pri] = std::move(*ret);

    auto signer = util::OpenSSLED25519::NewFromPemString(pri, {});
    auto validator = util::OpenSSLED25519::NewFromPemString(pub, {});

    auto data = std::string_view("562d6ddfb3ceb5abb12d97bc35c4963d249f55b7c75eda618d365492ee98d469");

    auto signatureRet = signer->sign(data.data(), data.size());
    if (!signatureRet) {
        return;
    }
    auto signature = *signatureRet;
    ASSERT_TRUE(validator->verify(signature, data.data(), data.size()) == 1) << ERR_error_string(ERR_get_error(), nullptr);
}

TEST_F(CryptoTest, TestED25519SaveAndLoadWithPasswd) {
    const auto data = std::string("562d6ddfb3ceb5abb12d97bc35c4963d249f55b7c75eda618d365492ee98d469");

    util::OpenSSLED25519::initCrypto();
    auto ret = util::OpenSSLED25519::generateKeyFiles(data+".pub", data+".pri", data);
    if(!ret) {
        return;
    }

    auto signer = util::OpenSSLED25519::NewFromPemFile(data+".pri", data);
    auto validator = util::OpenSSLED25519::NewFromPemFile(data+".pub", {});

    auto signatureRet = signer->sign(data.data(), data.size());
    if (!signatureRet) {
        return;
    }
    auto signature = *signatureRet;
    ASSERT_TRUE(validator->verify(signature, data.data(), data.size()) == 1) << ERR_error_string(ERR_get_error(), nullptr);
}

// Testcase from https://cryptobook.nakov.com/digital-signatures/eddsa-sign-verify-examples
TEST_F(CryptoTest, TestED25519Verify) {
    std::string pemPriKey =util::OpenSSLED25519::toHex("1498b5467a63dffa2dc9d9e069caf075d16fc33fdd4c3b01bfadae6433767d93");
    std::string pemPubKey =util::OpenSSLED25519::toHex("b7a3c12dc0c8c748ab07525b701122b88bd78f600c76342d27f25e5f92444cde");

    auto msg = std::string("Message for Ed25519 signing");
    auto sig = util::OpenSSLED25519::toHex("6dd355667fae4eb43c6e0ab92e870edb2de0a88cae12dbd8591507f584fe4912babff497f1b8edf9567d2483d54ddc6459bea7855281b7a246a609e3001a4e08");

    util::OpenSSLED25519::initCrypto();
    // Init signer and validator
    auto signer = util::OpenSSLED25519::NewPrivateKeyFromHex(pemPriKey);
    if(!signer) {
        ASSERT_TRUE(false) << "signer init error";
    }
    auto validator = util::OpenSSLED25519::NewPublicKeyFromHex(pemPubKey);
    if(!validator) {
        ASSERT_TRUE(false) << "validator init error";
    }

    // SIgn message
    auto signatureRet = signer->sign(msg.data(), msg.size());
    if (!signatureRet) {
        return;
    }
    auto signature = *signatureRet;
    std::copy(sig.begin(), sig.end(), signature.data());
    if(!validator->verify(signature, msg.data(), msg.size())) {
        ASSERT_TRUE(false) << "Validator verify error";
    }

    OpenSSL::digestType<64> md;
    std::copy(sig.begin(), sig.end(), md.data());
    if(!validator->verify(md, msg.data(), msg.size())) {
        ASSERT_TRUE(false) << "Validator verify error";
    }
}