//
// Created by peng on 2/11/23.
//

#ifndef NBP_CLUSTER_H
#define NBP_CLUSTER_H

#include "common/raft/fsm.h"
#include "common/raft/node_closure.h"

namespace util::raft {
    // a single raft cluster
    class Cluster {
    public:
        // name: the cluster name
        // peers: peers involved the cluster
        // election_timeout_ms and max_clock_drift_ms: for node options
        Cluster(std::string name, const std::vector<braft::PeerId> &peers,
                int32_t election_timeout_ms = 3000, int max_clock_drift_ms = 1000)
                : _name(std::move(name))
                , _peers(peers)
                , _election_timeout_ms(election_timeout_ms)
                , _max_clock_drift_ms(max_clock_drift_ms) {
            int64_t throttle_throughput_bytes = 10 * 1024 * 1024;
            int64_t check_cycle = 10;
            _throttle = new braft::ThroughputSnapshotThrottle(throttle_throughput_bytes, check_cycle);
        }

        ~Cluster() {
            stop_all();
        }

        int start(const butil::EndPoint &listen_addr, bool empty_peers = false, int snapshot_interval_s = 30, braft::Closure *leader_start_closure = nullptr) {
            if (_server_map[listen_addr] == nullptr) {
                auto *server = new brpc::Server();
                if (braft::add_service(server, listen_addr) != 0 || server->Start(listen_addr, nullptr) != 0) {
                    LOG(ERROR) << "Fail to start raft service";
                    delete server;
                    return -1;
                }
                _server_map[listen_addr] = server;
            }

            braft::NodeOptions options;
            options.election_timeout_ms = _election_timeout_ms;
            options.max_clock_drift_ms = _max_clock_drift_ms;
            options.snapshot_interval_s = snapshot_interval_s;
            if (!empty_peers) {
                options.initial_conf = braft::Configuration(_peers);
            }
            auto* fsm = new SingleRaftFSM(listen_addr);
            if (leader_start_closure) {
                fsm->set_on_leader_start_closure(leader_start_closure);
            }
            options.fsm = fsm;
            options.node_owns_fsm = true;
            butil::string_printf(&options.log_uri, "local://./data/%s/log", butil::endpoint2str(listen_addr).c_str());
            butil::string_printf(&options.raft_meta_uri, "local://./data/%s/raft_meta", butil::endpoint2str(listen_addr).c_str());
            butil::string_printf(&options.snapshot_uri, "local://./data/%s/snapshot", butil::endpoint2str(listen_addr).c_str());

            scoped_refptr<braft::SnapshotThrottle> tst(_throttle);
            options.snapshot_throttle = &tst;

            options.catchup_margin = 2;

            auto *node = new braft::Node(_name, braft::PeerId(listen_addr, 0));
            int ret = node->init(options);
            if (ret != 0) {
                LOG(WARNING) << "init_node failed, server: " << listen_addr;
                delete node;
                return ret;
            } else {
                LOG(INFO) << "init node " << listen_addr;
            }

            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            _nodes.push_back(node);
            _fsms.push_back(fsm);
            return 0;
        }

        int stop(const butil::EndPoint &listen_addr) {
            bthread::CountdownEvent cond;
            braft::Node *node = remove_node(listen_addr);
            if (node) {
                node->shutdown(NEW_CLOSURE(&cond));
                cond.wait();
                node->join();
            }

            if (_server_map[listen_addr] != nullptr) {
                delete _server_map[listen_addr];
                _server_map.erase(listen_addr);
            }
            _server_map.erase(listen_addr);
            delete node;
            return node ? 0 : -1;
        }

        void stop_all() {
            std::vector<butil::EndPoint> addrs;
            all_nodes(&addrs);

            for (const auto & addr : addrs) {
                stop(addr);
            }
        }

        static void clean(const butil::EndPoint &listen_addr) {
            std::string data_path;
            butil::string_printf(&data_path, "./data/%s", butil::endpoint2str(listen_addr).c_str());

            if (!butil::DeleteFile(butil::FilePath(data_path), true)) {
                LOG(ERROR) << "delete path failed, path: " << data_path;
            }
        }

        braft::Node* leader() {
            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            braft::Node *node = nullptr;
            for (size_t i = 0; i < _nodes.size(); i++) {
                if (_nodes[i]->is_leader()) {   //  && _fsms[i]->_leader_term == _nodes[i]->_impl->_current_term
                    node = _nodes[i];
                    break;
                }
            }
            return node;
        }

        void followers(std::vector<braft::Node *> *nodes) {
            nodes->clear();

            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            for (auto & _node : _nodes) {
                if (!_node->is_leader()) {
                    nodes->push_back(_node);
                }
            }
        }

        void all_nodes(std::vector<braft::Node *> *nodes) {
            nodes->clear();

            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            for (auto _node : _nodes) {
                nodes->push_back(_node);
            }
        }

        braft::Node *find_node(const braft::PeerId &peer_id) {
            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            for (auto & _node : _nodes) {
                if (peer_id == _node->node_id().peer_id) {
                    return _node;
                }
            }
            return nullptr;
        }

        void wait_leader() {
            while (true) {
                braft::Node *node = leader();
                if (node) {
                    return;
                } else {
                    usleep(100 * 1000);
                }
            }
        }

        void check_node_status() {
            std::vector<braft::Node *> nodes;
            {
                std::lock_guard<braft::raft_mutex_t> guard(_mutex);
                for (auto _node : _nodes) {
                    nodes.push_back(_node);
                }
            }
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

        void ensure_leader(const butil::EndPoint &expect_addr) {
CHECK:
            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            for (auto & _node : _nodes) {
                braft::PeerId leader_id = _node->leader_id();
                if (leader_id.addr != expect_addr) {
                    goto WAIT;
                }
            }
            return;
WAIT:
            usleep(100 * 1000);
            goto CHECK;
        }

        bool ensure_same(int wait_time_s = -1) {
            std::unique_lock<braft::raft_mutex_t> guard(_mutex);
            if (_fsms.size() <= 1) {
                return true;
            }
            LOG(INFO) << "_fsms.size()=" << _fsms.size();

            int nround = 0;
            auto* first = _fsms[0];
CHECK:
            const auto& first_logs = first->get_logs().get_log_for_read();
            for (size_t i = 1; i < _fsms.size(); i++) {
                auto* fsm = _fsms[i];
                const auto& fsm_logs = fsm->get_logs().get_log_for_read();
                if (first_logs.size() != fsm_logs.size()) {
                    LOG(INFO) << "logs size not match, "
                              << " addr: " << first->get_address() << " vs "
                              << fsm->get_address() << ", log num "
                              << first_logs.size() << " vs " << fsm_logs.size();
                    fsm->get_logs().restore_read();
                    goto WAIT;
                }

                for (size_t j = 0; j < first_logs.size(); j++) {
                    const auto& first_data = first_logs[j];
                    const auto& fsm_data = fsm_logs[j];
                    if (first_data.to_string() != fsm_data.to_string()) {
                        LOG(INFO) << "log data of index=" << j << " not match, "
                                  << " addr: " << first->get_address() << " vs "
                                  << fsm->get_address() << ", data ("
                                  << first_data.to_string() << ") vs "
                                  << fsm_data.to_string() << ")";
                        fsm->get_logs().restore_read();
                        goto WAIT;
                    }
                }
                fsm->get_logs().restore_read();
            }
            first->get_logs().restore_read();
            guard.unlock();
            check_node_status();
            return true;
WAIT:
            first->get_logs().restore_read();
            sleep(1);
            ++nround;
            if (wait_time_s > 0 && nround > wait_time_s) {
                return false;
            }
            goto CHECK;
        }

    private:
        void all_nodes(std::vector<butil::EndPoint> *addrs) {
            addrs->clear();

            std::lock_guard<braft::raft_mutex_t> guard(_mutex);
            for (auto & _node : _nodes) {
                addrs->push_back(_node->node_id().peer_id.addr);
            }
        }

        braft::Node *remove_node(const butil::EndPoint &addr) {
            std::lock_guard<braft::raft_mutex_t> guard(_mutex);

            // remove node
            braft::Node *node = nullptr;
            std::vector<braft::Node *> new_nodes;
            for (auto & _node : _nodes) {
                if (addr.port == _node->node_id().peer_id.addr.port) {
                    node = _node;
                } else {
                    new_nodes.push_back(_node);
                }
            }
            _nodes.swap(new_nodes);

            // remove fsm
            std::vector<SingleRaftFSM*> new_fsms;
            for (auto & _fsm : _fsms) {
                if (_fsm->get_address() != addr) {
                    new_fsms.push_back(_fsm);
                }
            }
            _fsms.swap(new_fsms);

            return node;
        }

        std::string _name;
        std::vector<braft::PeerId> _peers;
        std::vector<braft::Node*> _nodes;
        std::vector<SingleRaftFSM*> _fsms;
        std::map<butil::EndPoint, brpc::Server *> _server_map;
        int32_t _election_timeout_ms;
        int32_t _max_clock_drift_ms;
        braft::raft_mutex_t _mutex;
        braft::SnapshotThrottle *_throttle;
    };
}
#endif //NBP_CLUSTER_H
