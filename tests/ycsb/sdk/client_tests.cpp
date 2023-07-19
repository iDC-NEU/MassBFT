//
// Created by user on 23-7-13.
//
#include "ycsb/sdk/client_sdk.h"
#include "common/property.h"
#include "gtest/gtest.h"

class ClientSDKTest : public ::testing::Test {
public:
    ClientSDKTest() {
        ycsb::sdk::ClientSDK::InitSDKDependencies();
    }

protected:
    void SetUp() override { };

    void TearDown() override { };
};

std::string hashToString(size_t hashValue) {
    std::ostringstream oss;
    oss << std::hex << hashValue;
    return oss.str();
}

std::unique_ptr<proto::Envelop> addDataToBlockchain(ycsb::sdk::SendInterface* sender, std::string key, const std::string& value) {
    std::hash<std::string> hasher;
    size_t hashValue = hasher(value);
    std::string hashString = hashToString(hashValue);
    std::string data;
    zpp::bits::out out(data);
    std::vector<std::string> strings = {std::move(key), hashString};
    if (failure(out(strings))) {
        std::cout << "no";
    }
    auto ret = sender->invokeChaincode("session_store", "Set", data);
    if (!ret) {
        std::cout << "Failed to invoke chaincode!" << std::endl;
        return nullptr;
    }
    return ret;
}
bool verifydata(ycsb::sdk::ReceiveInterface* receiver, const std::string& key, const std::string& value) {
    std::hash<std::string> hasher;
    size_t hashValue = hasher(value);
    std::string hashString = hashToString(hashValue);
    std::cout << "hash:" << hashString << std::endl;
    int blockid = 0;
    while (true) {
        auto block = receiver->getBlock(0, blockid, 1000);
        if (!block) {
            break;
        }
        for (const auto &request: block->body.userRequests) {
            std::string_view payload = request->getPayload();
            std::string arg;
            proto::UserRequest u;
            zpp::bits::in in(payload);
            if (failure(in(u))) {
                LOG(WARNING) << "Payload deserialization failed!";
                continue;
            }
            arg = u.getArgs();
            std::vector<std::string_view> args;
            zpp::bits::in inArgs(arg);
            if (failure(inArgs(args))) {
                std::cout << "arg deserialization failed!" << std::endl;
                continue;
            }
            LOG(INFO) << "key: " << args[0] << " valuehash: " << args[1] << std::endl;
            if (key != args[0]) {
                continue;   // not found
            }
            return hashString == args[1];
        }
        blockid++;
    }
    return false;
}

TEST_F(ClientSDKTest, BasicTest1) {
    CHECK(util::Properties::LoadProperties("peer.yaml"));
    auto* p = util::Properties::GetProperties();
    LOG(INFO) << p->replicatorLowestPort();
    auto clientSDK = ycsb::sdk::ClientSDK::NewSDK(*p);
    ASSERT_TRUE(clientSDK);
    ASSERT_TRUE(clientSDK->connect());
    ycsb::sdk::SendInterface* sender = clientSDK.get();
    ycsb::sdk::ReceiveInterface* receiver = clientSDK.get();
    // wait until server ready
    std::this_thread::sleep_for(std::chrono::seconds(1));

    //insert key and value hash
    std::string key = "key";
    std::string value = "value";
    std::string key1 = "key1";
    std::string value1 = "value1";
    auto ret = addDataToBlockchain(sender, key, value);
    ASSERT_TRUE(ret);
    auto ret1 = addDataToBlockchain(sender, key1, value1);
    ASSERT_TRUE(ret1);
    char charArray[] = "value1";
    std::string str(charArray);
    ASSERT_TRUE(verifydata(receiver, key, value));
    ASSERT_TRUE(verifydata(receiver, key1, str));
}