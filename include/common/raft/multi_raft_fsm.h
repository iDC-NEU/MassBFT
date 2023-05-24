//
// Created by peng on 2/13/23.
//

#pragma once

#include "common/raft/raft_fsm.h"
#include "common/raft/node_closure.h"
#include <memory>
#include <shared_mutex>
#include <mutex>
#include <memory>

namespace util::raft {
    // A multi-raft instance
    // name: the cluster name
    class MultiRaftFSM {
    public:
        explicit MultiRaftFSM(std::string_view group_name)
                : _name(group_name) {
            int64_t throttle_throughput_bytes = 10 * 1024 * 1024;
            int64_t check_cycle = 10;
            _throttle = new braft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
        }

        ~MultiRaftFSM() {
            stop_all();
        }

        // start a new raft group, return 0 on success
        // note: peers must include local peer
        // the fsm will be taken by braft::Node
        int start(const std::vector<braft::PeerId>& peers, int local_peer_index, SingleRaftFSM *fsm = nullptr) {
            // index is not out of range
            if (local_peer_index >= (int)peers.size() || local_peer_index < 0) {
                LOG(WARNING) << "init_node failed, peers index out of range.";
                return -1;
            }
            auto& local_peer = peers[local_peer_index];
            // the group is not already started
            if (find_node(local_peer) != nullptr) {
                LOG(WARNING) << "the peer is already started.";
                return -1;
            }
            auto options = std::make_unique<braft::NodeOptions>();
            options->election_timeout_ms = _election_timeout_ms;
            options->max_clock_drift_ms = _max_clock_drift_ms;
            options->snapshot_interval_s = _snapshot_interval_s;

            options->initial_conf = braft::Configuration(peers);

            if (fsm == nullptr) {
                fsm = new SingleRaftFSM();
            }
            fsm->set_address(local_peer.addr);
            options->fsm = fsm;
            options->node_owns_fsm = true;
            butil::string_printf(&options->log_uri, "local://./data/%s/log", local_peer.to_string().c_str());
            butil::string_printf(&options->raft_meta_uri, "local://./data/%s/raft_meta", local_peer.to_string().c_str());
            butil::string_printf(&options->snapshot_uri, "local://./data/%s/snapshot", local_peer.to_string().c_str());

            scoped_refptr<braft::SnapshotThrottle> tst(_throttle);
            options->snapshot_throttle = &tst;
            options->catchup_margin = 2;

            auto *node = new braft::Node(_name, local_peer);
            int ret = node->init(*options);
            if (ret != 0) {
                LOG(WARNING) << "init_node failed, server: " << local_peer.to_string();
                delete node;
                return ret;
            } else {
                LOG(INFO) << "init node " << local_peer.to_string();
            }

            std::unique_lock guard(_mutex);
            _instances[local_peer] = raft_instance{node, fsm, std::move(options)};
            return 0;
        }

        // stop the local peer, return 0 on success, -1 on error
        int stop(const braft::PeerId &peer) {
            bthread::CountdownEvent cond;
            braft::Node *node = remove_node(peer);
            if (!node) {    // node does not exist
                return -1;
            }
            node->shutdown(NEW_CLOSURE(&cond));
            cond.wait();
            node->join();
            delete node;
            return 0;
        }

        void stop_all() {
            std::vector<braft::PeerId> peers;
            all_nodes(&peers);
            for (const auto & peer : peers) {
                stop(peer);
            }
        }

        // clean log of specific instance
        static void clean(const braft::PeerId& peer) {
            std::string data_path;
            butil::string_printf(&data_path, "./data/%s", peer.to_string().c_str());
            if (!butil::DeleteFile(butil::FilePath(data_path), true)) {
                LOG(ERROR) << "delete path failed, path: " << data_path;
            }
        }

        braft::Node* find_node(const braft::PeerId& peer) {
            std::shared_lock guard(_mutex);
            if (auto iter=_instances.find(peer); iter!=_instances.end()) {
                return iter->second.node;
            }
            return nullptr;
        }

        // block until the peer connected to the leader
        static braft::PeerId wait_leader(braft::Node *node, int retry_timeout=100 * 1000) {
            while (true) {
                braft::PeerId leader = node->leader_id();
                if (!leader.is_empty()) {
                    return leader;
                }
                usleep(retry_timeout);
            }
        }

        // block until the expect_peer become leader
        static void ensure_leader(braft::Node* node, const braft::PeerId& expect_peer, int retry_timeout=100 * 1000) {
            while(true) {
                braft::PeerId leader_id = wait_leader(node, retry_timeout);
                if (leader_id == expect_peer) {
                    return;
                }
                usleep(retry_timeout);
            }
        }

        void check_node_status() {
            std::vector<braft::Node *> nodes;
            all_nodes(&nodes);
            for (auto & node : nodes) {
                braft::NodeStatus status;
                node->get_status(&status);
                if (node->is_leader()) {
                    CHECK(status.state == braft::STATE_LEADER);
                } else {
                    CHECK(status.state != braft::STATE_LEADER);
                    CHECK(status.stable_followers.empty());
                }
            }
        }

        braft::Node *remove_node(const braft::PeerId &peer) {
            std::unique_lock guard(_mutex);
            if (auto iter=_instances.find(peer); iter!=_instances.end()) {
                braft::Node *node = iter->second.node;   // fsm will be delete by node
                _instances.erase(iter);
                return node;
            }
            return nullptr;
        }

        void all_nodes(std::vector<braft::PeerId> *peers) {
            std::shared_lock guard(_mutex);
            peers->clear();
            peers->reserve(_instances.size());
            for (auto& it : _instances) {
                peers->push_back(it.first);
            }
        }

        void all_nodes(std::vector<braft::Node*> *nodes) {
            std::shared_lock guard(_mutex);
            nodes->clear();
            nodes->reserve(_instances.size());
            for (auto& it : _instances) {
                nodes->push_back(it.second.node);
            }
        }

        SingleRaftFSM* find_fsm(const braft::PeerId& peer) {
            std::shared_lock guard(_mutex);
            if (auto iter=_instances.find(peer); iter!=_instances.end()) {
                return iter->second.fsm;
            }
            LOG(ERROR) << "Can not find fsm of peer: " << peer;
            return nullptr;
        }

        const braft::NodeOptions* get_options(const braft::PeerId& peer) const {
            std::shared_lock guard(_mutex);
            if (auto iter=_instances.find(peer); iter!=_instances.end()) {
                return iter->second.options.get();
            }
            LOG(ERROR) << "Can not find option of peer: " << peer;
            return nullptr;
        }

    private:
        const std::string _name;
        // default node config
        const int32_t _election_timeout_ms = 3000;
        const int32_t _max_clock_drift_ms = 1000;
        const int _snapshot_interval_s = 3600;
        braft::SnapshotThrottle* _throttle;

        // vector of all local raft instance, and lock
        mutable std::shared_mutex _mutex;
        struct raft_instance {
            braft::Node* node{};
            SingleRaftFSM* fsm{};   // owns by node, do not need to free memory
            std::unique_ptr<braft::NodeOptions> options;
        };
        std::map<braft::PeerId, raft_instance> _instances;

    };
}
