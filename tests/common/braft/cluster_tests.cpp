//
// Created by peng on 2/10/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "common/raft/cluster.h"
#include "common/raft/node_closure.h"


class ClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(ClusterTest, TripleNode) {
    std::vector<braft::PeerId> peers;
    for (int i = 0; i < 3; i++) {
        braft::PeerId peer;
        peer.addr.ip = butil::my_ip();
        peer.addr.port = 5006 + i;
        peer.idx = 0;

        peers.push_back(peer);
    }

    // start cluster
    util::raft::Cluster cluster("unittest", peers);
    for (auto & peer : peers) {
        ASSERT_EQ(0, cluster.start(peer.addr));
    }

    // elect leader
    cluster.wait_leader();
    braft::Node* leader = cluster.leader();
    ASSERT_TRUE(leader != nullptr);
    LOG(WARNING) << "leader is " << leader->node_id();

    // apply something
    bthread::CountdownEvent cond(10);
    for (int i = 0; i < 10; i++) {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "hello: %d", i + 1);
        data.append(data_buf);

        braft::Task task;
        task.data = &data;
        task.done = NEW_CLOSURE_WITH_CODE(&cond, 0);
        leader->apply(task);
    }
    cond.wait();

    {
        butil::IOBuf data;
        char data_buf[128];
        snprintf(data_buf, sizeof(data_buf), "no closure");
        data.append(data_buf);
        braft::Task task;
        task.data = &data;
        leader->apply(task);
    }

    cluster.ensure_same();

    {
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = brpc::PROTOCOL_HTTP;

        if (channel.Init(leader->node_id().peer_id.addr, &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
        }

        {
            brpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat";
            cntl.http_request().set_method(brpc::HTTP_METHOD_GET);

            channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr/*done*/);

            LOG(INFO) << "http return: \n" << cntl.response_attachment();
        }

        {
            brpc::Controller cntl;
            cntl.http_request().uri() = "/raft_stat/unittest";
            cntl.http_request().set_method(brpc::HTTP_METHOD_GET);

            channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr/*done*/);

            LOG(INFO) << "http return: \n" << cntl.response_attachment();
        }
    }

    // stop cluster
    std::vector<braft::Node*> nodes;
    cluster.followers(&nodes);
    ASSERT_EQ(2, nodes.size());

    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}