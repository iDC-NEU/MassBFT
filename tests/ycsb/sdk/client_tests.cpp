//
// Created by user on 23-7-13.
//
#include "ycsb/sdk/client_sdk.h"
#include "tests/mock_property_generator.h"
#include "tests/peer/mock_peer.h"
#include "gtest/gtest.h"
#include <iostream>


class ClientSDKTest : public ::testing::Test {
protected:
  void SetUp() override {
    tests::MockPropertyGenerator::GenerateDefaultProperties(1, 3);
    tests::MockPropertyGenerator::SetLocalId(0, 0);
    util::Properties::SetProperties(util::Properties::BATCH_MAX_SIZE, 1);
    ycsb::sdk::ClientSDK::InitSDKDependencies();
  };

  void TearDown() override { };
};

std::string hashToString(size_t hashValue) {
  std::ostringstream oss;
  oss << std::hex << hashValue;
  return oss.str();
}

std::unique_ptr<proto::Envelop> addDataToBlockchain(ycsb::sdk::SendInterface* sender, std::string key,std::string value) {
  std::hash<std::string> hasher;
  size_t hashValue = hasher(value);
  std::string hashString = hashToString(hashValue);
  std::string data;
  zpp::bits::out out(data);
  std::vector<std::string> strings = {key, hashString};
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
void verifydata(ycsb::sdk::ReceiveInterface* receiver, std::string key, std::string value){
  std::hash<std::string> hasher;
  size_t hashValue = hasher(value);
  std::string hashString = hashToString(hashValue);
  std::cout << "hash:" << hashString << std::endl;
  int blockid = 0;
  int a = 0;
  while (true) {
    auto block = receiver->getBlock(0, blockid, 1000);
    if (!block) {
      break;
    }
    ASSERT_TRUE(block);
    ASSERT_TRUE(block->body.userRequests.size() == 1);
    for (const auto& request : block->body.userRequests) {
      std::string_view payload = request->getPayload();
      std::string arg;
      proto::UserRequest u;
      zpp::bits::in in(payload);
      if (failure(in(u))) {
        std::cout << "Payload deserialization failed!" << std::endl;
      } else {
        arg = u.getArgs();
        std::vector<std::string_view> args;
        zpp::bits::in inArgs(arg);
        if (failure(inArgs(args))) {
          std::cout << "arg deserialization failed!" << std::endl;
        }
        std::cout << "key: " << args[0] << " valuehash: " << args[1] << std::endl;
        if(key == args[0]){
          a = 1;
          if(hashString == args[1]){
            std::cout << "success! blockid:" << blockid << std::endl;
          }else{
            std::cout << "Validation failed! blockid:" << blockid << std::endl;
          }
          break;
        }
      }
    }
    blockid ++;
  }
  if (a == 0)
    std::cout<<"not found" <<std::endl;

}

TEST_F(ClientSDKTest, BasicTest1) {
  auto* p = util::Properties::GetProperties();
  tests::peer::Peer peer(*p, false, false);
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
  verifydata(receiver,key,value);
  verifydata(receiver,key1,str);
}