//
// Created by peng on 2/10/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "common/raft/multi_raft_cluster.h"
#include "common/raft/node_closure.h"
#include "common/thread_pool_light.h"


class ClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static std::vector<braft::PeerId> preparePeers(int count, int groupId = 0) {
        std::vector<braft::PeerId> peers;
        for (int i = 0; i < count; i++) {
            braft::PeerId peer;
            peer.addr.ip = butil::my_ip();
            peer.addr.port = 5006 + i;
            peer.idx = groupId;

            peers.push_back(peer);
        }
        return peers;
    }

    void initRaftGroup(util::raft::Cluster* cluster, const std::vector<braft::PeerId>& peers) {
        std::lock_guard lock(mutex);
        ASSERT_EQ(0, cluster->start_raft_group(peers));
    }

    // the start progress must be in sequence
    static void startRaftGroupTest(util::raft::Cluster* cluster, const std::vector<braft::PeerId>& peers) {
        auto& pk_peer = peers[0];    // primary key, represent the whole group

        // elect leader
        cluster->wait_leader(pk_peer);
        braft::Node* leader = cluster->leader(pk_peer);
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

        cluster->ensure_same(pk_peer);

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
        std::vector<braft::Node*> nodes = cluster->followers(pk_peer);
        ASSERT_EQ(peers.size()-1, nodes.size());
    }

private:
    std::mutex mutex;

};

TEST_F(ClusterTest, TripleNode) {
    std::unique_ptr<util::thread_pool_light> wp = std::make_unique<util::thread_pool_light>((int) sysconf(_SC_NPROCESSORS_ONLN));
    util::raft::Cluster cluster("TripleNode multi raft testcase");
    for(int i=4; i<6; i++) {
        std::vector<braft::PeerId> peers = preparePeers(3, i);
        initRaftGroup(&cluster, peers);
    }

    bthread::CountdownEvent cond(2);
    for(int i=4; i<6; i++) {
        wp->push_task([&, i=i]{
            std::vector<braft::PeerId> peers = preparePeers(3, i);
            startRaftGroupTest(&cluster, peers);
            cond.signal();
        });
    }
    cond.wait();
    // stop cluster
    LOG(WARNING) << "cluster stop";
    cluster.stop_all();
}