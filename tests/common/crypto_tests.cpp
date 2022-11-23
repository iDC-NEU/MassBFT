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
    OpenSSL::initOpenSSLCrypto();
    OpenSSL::digestType defaultMsg;
    ASSERT_TRUE(OpenSSL::bytesToString(util::OpenSSLSHA1::generateDigest(msg).value_or(defaultMsg)) == kat1) << "SHA1 FAIL";
    ASSERT_TRUE(OpenSSL::bytesToString(util::OpenSSLSHA256::generateDigest(msg).value_or(defaultMsg)) == kat256) << "SHA256 FAIL";
}

TEST_F(CryptoTest, TestDynamicFunc) {
    OpenSSL::initOpenSSLCrypto();
    OpenSSL::digestType defaultMsg;
    auto hash = util::OpenSSLSHA256();
    ASSERT_TRUE(hash.update(msg)) << "update failed";
    ASSERT_TRUE(OpenSSL::bytesToString(hash.final().value_or(defaultMsg)) == kat256) << "SHA256 FAIL";
    std::string sv = msg;
    ASSERT_TRUE(hash.update(sv.substr(0,10))) << "update failed";
    ASSERT_TRUE(OpenSSL::bytesToString(hash.updateFinal(sv.substr(10)).value_or(defaultMsg)) == kat256) << "SHA256 FAIL";
}

TEST_F(CryptoTest, SingleThreadPerformance) {
    OpenSSL::initOpenSSLCrypto();
    OpenSSL::digestType defaultMsg;
    auto hash = util::OpenSSLSHA256();
    std::string dataEncode; // each thread use a different data
    for (int i=0; i<200000; i++) {
        dataEncode += std::to_string(i*3);
    }
    LOG(INFO) << "Data Size: " << dataEncode.size();
    util::Timer timer;
    for (int i=0; i<1000; i++) {
        hash.updateFinal(dataEncode);
    }
    LOG(INFO) << "Total Time spend: " << timer.end();
}