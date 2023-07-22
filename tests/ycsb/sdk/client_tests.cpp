//
// Created by user on 23-7-13.
//
#include "ycsb/sdk/client_sdk.h"
#include "common/property.h"
#include "gtest/gtest.h"
#include "proto/client.pb.h"
#include "common/meta_rpc_server.h"

std::unique_ptr<proto::Envelop> addDataToBlockchain(ycsb::sdk::SendInterface* sender, std::string key, const std::string& value);
std::string verifyDATA(ycsb::sdk::ReceiveInterface* receiver, const std::string& key, const std::string& value);


class CLIENTServiceImpl : public client::proto::CLIENTService {

private:
    ycsb::sdk::SendInterface* sender_;
    ycsb::sdk::ReceiveInterface* receiver_;

public:
    CLIENTServiceImpl(ycsb::sdk::SendInterface* sender, ycsb::sdk::ReceiveInterface* receiver)
            : sender_(sender), receiver_(receiver) {}
    // 实现插入数据的RPC方法
    void InsertData(::google::protobuf::RpcController *,
                    const ::client::proto::InsertRequest* request,
                    ::client::proto::InsertResponse* response,
                    ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::cout << "Received InsertData request with key: " << request->key() << std::endl;

        // 在这里处理插入数据的逻辑，你可以将数据存储到数据库或其他存储介质中
        // 调用 addDataToBlockchain 方法
        auto ret = addDataToBlockchain(sender_, request->key(), request->value());
        if (!ret) {
            std::cout << "Failed to add data to the blockchain!" << std::endl;
            response->set_success(false);
            return;
        }
        response->set_success(true);
    }

    // 实现查询数据的RPC方法
    void QueryData(::google::protobuf::RpcController *,
                   const ::client::proto::QueryRequest* request,
                   ::client::proto::QueryResponse* response,
                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::cout << "Received QueryData request with key: " << request->key() << std::endl;

        // 在这里处理查询数据的逻辑，根据请求的key找到对应的数据
        std::string verified = verifyDATA(receiver_, request->key(), request->value());

        response->set_hash("hash");
        response->set_message(verified);
    }
};


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
    brpc::Server server;

    // 创建服务实例
    CLIENTServiceImpl client_service_impl(sender, receiver);

    // 添加服务到服务器
    if (server.AddService(&client_service_impl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(INFO) << "添加服务失败" ;
    }

    // 设置服务器监听的IP地址和端口
    brpc::ServerOptions options;
    options.idle_timeout_sec = 60; // 设置空闲超时时间，60秒
    if (server.Start(9000, &options) != 0) {
        LOG(INFO) << "启动服务器失败" ;
    }
    LOG(INFO) << "启动服务器" ;
    // 运行服务器，直到接收到SIGINT或SIGTERM信号
    server.RunUntilAskedToQuit();
}