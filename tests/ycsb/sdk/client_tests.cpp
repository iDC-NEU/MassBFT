//
// Created by user on 23-7-13.
//
#include "ycsb/sdk/httplib.h"
#include "common/property.h"
#include "gtest/gtest.h"
#include "proto/client.pb.h"
#include "common/meta_rpc_server.h"
#include "ycsb/sdk/client_sdk.h"
#include <nlohmann/json.hpp>
using json = nlohmann::json;
std::unique_ptr<proto::Envelop> addDataToBlockchain(ycsb::sdk::SendInterface* sender, std::string key, const std::string& value);
std::string verifyDATA(ycsb::sdk::ReceiveInterface* receiver, const std::string& key, const std::string& value);


class ClientSDKTest : public ::testing::Test {
public:
    ClientSDKTest() {
        ycsb::sdk::ClientSDK::InitSDKDependencies();
    }

protected:
    void SetUp() override { };

    void TearDown() override { };
};

void handle_post(const httplib::Request &req, httplib::Response &res, ycsb::sdk::SendInterface* sender) {
    json body;
    try {
        body = json::parse(req.body);
    } catch (const json::parse_error &e) {
        res.status = 400;
        res.set_content("Invalid JSON data", "text/plain");
        return;
    }

    if (body.contains("key") && body.contains("value")) {
        std::string key = body["key"];
        std::string value = body["value"];

        // 调用 addDataToBlockchain 函数来向区块链中添加数据
        auto ret = addDataToBlockchain(sender, key, value);
        if (!ret) {
            json response;
            response["success"] = false;
            response["message"] = "Failed to add data to blockchain";
            res.status = 500;
            res.set_content(response.dump(), "application/json");
            return;
        }

        std::string message = "You entered into block Key: " + key + " and Value: " + value;
        json response;
        response["success"] = true;
        response["message"] = message;
        res.set_content(response.dump(), "application/json");
    } else {
        json response;
        response["success"] = false;
        response["message"] = "Invalid request. Missing key or value.";
        res.status = 400;
        res.set_content(response.dump(), "application/json");
    }
}

void handle_verify_post(const httplib::Request &req, httplib::Response &res, ycsb::sdk::ReceiveInterface* receiver) {
    json body;
    try {
        body = json::parse(req.body);
    } catch (const json::parse_error &e) {
        res.status = 400;
        res.set_content("Invalid JSON data", "text/plain");
        return;
    }

    if (body.contains("key") && body.contains("value")) {
        std::string key = body["key"];
        std::string value = body["value"];

        // 调用 verifyDATA 函数来验证区块链中的数据
        std::string verificationResult = verifyDATA(receiver, key, value);

        json response;
        response["success"] = true;
        response["message"] = verificationResult;

        res.set_content(response.dump(), "application/json");
    } else {
        json response;
        response["success"] = false;
        response["message"] = "Invalid request. Missing key or value.";
        res.status = 400;
        res.set_content(response.dump(), "application/json");
    }
}


std::string hashToString(size_t hashValue) {
    std::ostringstream oss;
    oss << std::hex << hashValue;
    return oss.str();
}

std::unique_ptr<proto::Envelop> addDataToBlockchain(ycsb::sdk::SendInterface* sender, std::string key, const std::string& value) {
    std::hash<std::string> hasher;
    util::OpenSSLSHA256::initCrypto();
    auto hash = util::OpenSSLSHA256();
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
    LOG(INFO) << "SUCCESS insert key:" << value ;
    return ret;
}
std::string verifyDATA(ycsb::sdk::ReceiveInterface* receiver, const std::string& key, const std::string& value) {
    std::hash<std::string> hasher;
    size_t hashValue = hasher(value);
    std::string hashString = hashToString(hashValue);
    std::cout << "hash:" << hashString << std::endl;
    int blockID = 0;
    while (true) {
        auto block = receiver->getBlock(0, blockID, 400);
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
                LOG(WARNING) << "arg deserialization failed!";
                continue;
            }
            LOG(INFO) << "key: " << args[0] << " value hash: " << args[1] << std::endl;
            if (key != args[0]) {
                continue;   // not found
            }
            if (hashString != args[1]) {
                return "data missmatch";
            }
            return hashString;
        }
        blockID++;
    }
    return "data not found";
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
    /*
    std::string key = "key";
    std::string value = "value";
    std::string key1 = "key1";
    std::string value1 = "value1";
    auto ret = addDataToBlockchain(sender, key, value);
    ASSERT_TRUE(ret);
    auto ret1 = addDataToBlockchain(sender, key1, value1);
    ASSERT_TRUE(ret1);
    ASSERT_TRUE(verifyDATA(receiver, key, value));
    ASSERT_TRUE(verifyDATA(receiver, key1, value1));
     */
    // 初始化brpc服务器
    auto ret = addDataToBlockchain(sender, "key1", "value1");
    auto ret3 = addDataToBlockchain(sender, "key17", "value17");

    httplib::Server server;
    auto ret1 = sender->invokeChaincode("ycsb", "w", "args1");
    ASSERT_TRUE(ret1);
    auto ret2 = sender->invokeChaincode("ycsb", "w", "args2");
    ASSERT_TRUE(ret2);
    server.Post("/block/submit-data", [&](const httplib::Request &req, httplib::Response &res) {
        handle_post(req, res, sender);
    });
    server.Post("/block/verify-data", [&](const httplib::Request &req, httplib::Response &res) {
        handle_verify_post(req, res, receiver); // 将 receiver 传递给 handle_verify_post
    });


    if (server.listen("0.0.0.0", 8091)) {
        std::cout << "Server started at http://localhost:8091/" << std::endl;
    } else {
        std::cerr << "Failed to start the server" << std::endl;
    }
}