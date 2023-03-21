//
// Created by user on 23-3-21.
//

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "peer/consensus/pbft/mock_rpc_server.h"

class SimpleRPCServerTest : public ::testing::Test {
    void SetUp() override {
        peer::consensus::MockRPCServer::AddRPCService();
    };

    void TearDown() override {
    };
};

TEST_F(SimpleRPCServerTest, TestStartServer) {
    constexpr int port = 9500;
    util::MetaRpcServer::Start<port>();
    sleep(3600);
}