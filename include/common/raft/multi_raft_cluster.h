//
// Created by peng on 2/11/23.
//

#ifndef NBP_MULTI_RAFT_CLUSTER_H
#define NBP_MULTI_RAFT_CLUSTER_H

#include "common/raft/multi_raft_fsm.h"

namespace util::raft {
    // Multi-raft cluster run in local demo, do not use this class in distributed settings.
    class Cluster {
    public:
        // name: the cluster name
        explicit Cluster(std::string_view cluster_name) : _name(cluster_name) { }

        ~Cluster() {
            stop_all();
        }

        int start_raft_group(const std::vector<braft::PeerId> &peers, braft::Closure *leader_start_closure = nullptr) {
            for (const auto& peer: peers) {
                if (_server_map[peer.addr] == nullptr) {
                    auto *server = new brpc::Server();
                    if (braft::add_service(server, peer.addr) != 0 || server->Start(peer.addr, nullptr) != 0) {
                        LOG(ERROR) << "Fail to start raft service";
                        delete server;
                        return -1;
                    }
                    _server_map[peer.addr] = server;
                    _fsms[peer.addr] = new MultiRaftFSM(_name);
                }
            }
            for (int i=0; i<(int)peers.size(); i++) {
                MultiRaftFSM* fsm = _fsms[peers[i].addr];
                if (int ret = fsm->start(peers, i, leader_start_closure); ret != 0) {
                    LOG(ERROR) << "Fail to init fsm";
                    return ret;
                }
            }
            return 0;
        }

        braft::Node* find_node(const braft::PeerId& peer) {
            auto* parent_fsm = _fsms[peer.addr];
            if (parent_fsm == nullptr) {
                LOG(ERROR) << "Fsm owns this instance not found!";
                return nullptr;
            }
            return parent_fsm->find_node(peer);
        }

        int stop_raft_group(const braft::PeerId& peer) {
            std::vector<braft::PeerId> peers;
            _fsms[peer.addr]->get_options(peer)->initial_conf.list_peers(&peers);
            for (const auto& it: peers) {
                auto* parent_fsm = _fsms[it.addr];
                if (parent_fsm == nullptr) {
                    LOG(ERROR) << "Fsm owns this instance not found!";
                    return -1;
                }
                parent_fsm->stop(peer);
            }
            return 0;
        }

        void stop_all() {
            for (auto& fsm: _fsms) {
                fsm.second->stop_all();
            }
        }

        // return the leader of a raft group
        braft::Node* leader(const braft::PeerId& peer) {
            std::vector<braft::PeerId> peers;
            auto leader_id = _fsms[peer.addr]->find_node(peer)->leader_id();
            if (!leader_id.is_empty()) {
                if (auto *node = find_node(leader_id); node != nullptr && node->is_leader()) {
                    return node;
                }
            }
            return nullptr;
        }

        std::vector<braft::Node*> followers(const braft::PeerId& peer) {
            std::vector<braft::PeerId> peers;
            _fsms[peer.addr]->get_options(peer)->initial_conf.list_peers(&peers);
            std::vector<braft::Node*> nodes;
            nodes.reserve(peers.size() - 1);
            for (const auto &it: peers) {
                if (auto *node = find_node(it); node != nullptr && !node->is_leader()) {
                    nodes.push_back(node);
                }
            }
            return nodes;
        }

        std::vector<braft::Node*> all_nodes(const braft::PeerId& peer) {
            std::vector<braft::PeerId> peers;
            _fsms[peer.addr]->get_options(peer)->initial_conf.list_peers(&peers);
            std::vector<braft::Node*> nodes;
            nodes.reserve(peers.size() - 1);
            for (const auto &it: peers) {
                auto *node = find_node(it);
                if (node == nullptr) {
                    LOG(WARNING) << "Node not found!";
                }
                nodes.push_back(node);
            }
            return nodes;
        }

        void wait_leader(const braft::PeerId& peer) {
            while (true) {
                braft::Node *node = leader(peer);
                if (node) {
                    return;
                } else {
                    usleep(100 * 1000);
                }
            }
        }

        void check_node_status() {
            for (auto& fsm: _fsms) {
                fsm.second->check_node_status();
            }
        }

        void ensure_leader(const braft::PeerId& peer) {
            auto* fsm = _fsms[peer.addr];
            MultiRaftFSM::ensure_leader(fsm->find_node(peer), peer);
        }

        SingleRaftFSM* find_fsm(const braft::PeerId& peer) {
            auto* parent_fsm = _fsms[peer.addr];
            if (parent_fsm == nullptr) {
                LOG(ERROR) << "Fsm owns this instance not found!";
                return nullptr;
            }
            return parent_fsm->find_fsm(peer);
        }

        bool ensure_same(const braft::PeerId& peer, int wait_time_s = -1) {
            if (_fsms.size() <= 1) {
                return true;
            }
            LOG(INFO) << "_fsms.size()=" << _fsms.size();
            std::vector<braft::PeerId> peers;
            _fsms[peer.addr]->get_options(peer)->initial_conf.list_peers(&peers);

            int nround = 0;
            auto* first = find_fsm(peers[0]);
CHECK:
            const auto& first_logs = first->get_logs().get_log_for_read();
            for (size_t i = 1; i < _fsms.size(); i++) {
                auto* fsm = find_fsm(peers[i]);
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
        std::string _name;
        std::map<butil::EndPoint, MultiRaftFSM*> _fsms;
        std::map<butil::EndPoint, brpc::Server*> _server_map;
    };
}
#endif //NBP_MULTI_RAFT_CLUSTER_H
