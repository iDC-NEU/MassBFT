//
// Created by peng on 11/29/22.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "common/bccsp.h"
#include "common/thread_pool_light.h"

#include <vector>

class BCCSPTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(BCCSPTest, IntrgrateTest) {
    auto ms = std::make_unique<util::DefaultKeyStorage>();
    auto* msPtr = ms.get();
    util::BCCSP bccsp(std::move(ms));
    auto ski_1 = "test_ski_1";
    auto ski_2 = "test_ski_2";
    auto ski_3 = "test_ski_3";
    auto key_1 = bccsp.generateED25519Key(ski_1, false);
    auto key_2 = bccsp.generateED25519Key(ski_2, true);
    ASSERT_TRUE(key_1 != nullptr && key_2 != nullptr);
    ASSERT_TRUE(msPtr->loadKey(ski_1));

    // test cache
    key_1 = bccsp.GetKey(ski_1);
    key_2 = bccsp.GetKey(ski_2);
    ASSERT_TRUE(key_1 != nullptr && key_2 != nullptr);

    // test incorrect ski
    ASSERT_TRUE(bccsp.GetKey(ski_3) == nullptr);

    // create hex format public key
    auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
    ASSERT_TRUE(ret);
    auto [pubPem, priPem] = std::move(*ret);

    auto pri = util::OpenSSLED25519::NewFromPemString(priPem, "");
    ASSERT_TRUE(pri);
    auto pubHexRet = pri->getHexFromPublicKey();
    ASSERT_TRUE(pubHexRet);
    auto pubHex = std::move(*pubHexRet);
    auto priHexRet = pri->getHexFromPrivateKey();
    ASSERT_TRUE(priHexRet);
    auto priHex = std::move(*priHexRet);
}

TEST_F(BCCSPTest, TestGetKeyPublic) {
    auto ms = std::make_unique<util::DefaultKeyStorage>();
    auto* msPtr = ms.get();
    util::BCCSP bccsp(std::move(ms));
    auto ski_3 = "test_ski_3";

    // create hex format public key
    auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
    ASSERT_TRUE(ret);
    auto [pubPem, priPem] = std::move(*ret);

    auto pri = util::OpenSSLED25519::NewFromPemString(priPem, "");
    ASSERT_TRUE(pri);
    auto pubHexRet = pri->getHexFromPublicKey();
    ASSERT_TRUE(pubHexRet);
    auto pubHex = std::move(*pubHexRet);
    auto priHexRet = pri->getHexFromPrivateKey();
    ASSERT_TRUE(priHexRet);
    auto priHex = std::move(*priHexRet);

    // test bccsp.GetKey
    msPtr->saveKey(ski_3, pubHex, false, true);
    auto key_3 = bccsp.GetKey(ski_3);
    ASSERT_TRUE(key_3->SKI() == ski_3);
    ASSERT_TRUE(key_3->Ephemeral() == false);
    ASSERT_TRUE(key_3->Private() == false);
    ASSERT_TRUE(key_3->PrivateBytes() == nullptr);
    ASSERT_TRUE(key_3->PublicBytes() != nullptr);
    ASSERT_TRUE(*key_3->PublicBytes() == pubHex);

    key_3 = bccsp.KeyImportPEM(ski_3, pubPem, true, false);
    ASSERT_TRUE(key_3 == nullptr);

    key_3 = bccsp.KeyImportPEM(ski_3, pubPem, false, false);
    ASSERT_TRUE(key_3 != nullptr);
    auto retKey3 = msPtr->loadKey(ski_3);
    ASSERT_TRUE(retKey3->first == pubHex);
    ASSERT_TRUE(retKey3->second == false);

    key_3 = bccsp.KeyImportPEM(ski_3, priPem, true, false);
    ASSERT_TRUE(key_3 != nullptr);
    ASSERT_TRUE(key_3->SKI() == ski_3);
    ASSERT_TRUE(key_3->Private() == true);
    ASSERT_TRUE(key_3->PrivateBytes() != nullptr);
    ASSERT_TRUE(*key_3->PrivateBytes() == priHex);
    retKey3 = msPtr->loadKey(ski_3);
    ASSERT_TRUE(retKey3->first == priHex);
    ASSERT_TRUE(retKey3->second == true);
}

TEST_F(BCCSPTest, TestGetKeyPrivate) {
    auto ms = std::make_unique<util::DefaultKeyStorage>();
    auto* msPtr = ms.get();
    util::BCCSP bccsp(std::move(ms));
    auto ski_3 = "test_ski_3";

    // create hex format public key
    auto ret = util::OpenSSLED25519::generateKeyFiles({}, {}, {});
    ASSERT_TRUE(ret);
    auto [pubPem, priPem] = std::move(*ret);

    auto pri = util::OpenSSLED25519::NewFromPemString(priPem, "");
    ASSERT_TRUE(pri);
    auto pubHexRet = pri->getHexFromPublicKey();
    ASSERT_TRUE(pubHexRet);
    auto pubHex = std::move(*pubHexRet);
    auto priHexRet = pri->getHexFromPrivateKey();
    ASSERT_TRUE(priHexRet);
    auto priHex = std::move(*priHexRet);

    // test bccsp.GetKey
    msPtr->saveKey(ski_3, priHex, true, true);
    auto key_3 = bccsp.GetKey(ski_3);
    ASSERT_TRUE(key_3->SKI() == ski_3);
    ASSERT_TRUE(key_3->Ephemeral() == false);
    ASSERT_TRUE(key_3->Private() == true);
    ASSERT_TRUE(key_3->PrivateBytes() != nullptr);
    ASSERT_TRUE(key_3->PublicBytes() != nullptr);
    ASSERT_TRUE(*key_3->PrivateBytes() == priHex);
    ASSERT_TRUE(*key_3->PublicBytes() == pubHex);
}
