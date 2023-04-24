//
// Created by user on 23-4-5.
//

#pragma once

#include "common/raft/multi_raft_fsm.h"
#include "common/property.h"

#include <string>
#include <memory>

namespace peer::consensus {

    class FollowerRaftFSM: public util::raft::SingleRaftFSM {
    public:
        FollowerRaftFSM(auto&& myId, auto&& leaderId, auto&& fsm) {
            _myId = std::forward<decltype(myId)>(myId);
            _leaderId = std::forward<decltype(leaderId)>(leaderId);
            _multiRaftFsm = std::forward<decltype(fsm)>(fsm);
        }

    protected:
        void on_leader_start(int64_t term) override {
            util::raft::SingleRaftFSM::on_leader_start(term);
            if (init) {
                _multiRaftFsm->find_node(_myId)->transfer_leadership_to(_leaderId);
            }
        }

        void on_stop_following(const ::braft::LeaderChangeContext& ctx) override {
            if (ctx.leader_id() == _leaderId) {
                // emit a view change
                LOG(ERROR) << "Remote leader contains error.";
            }
        }

        void on_start_following(const ::braft::LeaderChangeContext& ctx) override {
            if (ctx.leader_id() == _leaderId) {
                LOG(INFO) << "Start following remote leader.";
                init = false;
            }
        }

    private:
        bool init = true;
        braft::PeerId _myId;
        braft::PeerId _leaderId;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaftFsm;
    };

    class AsyncAgreement {
    public:
        void setLocalZMQConfig(std::shared_ptr<util::ZMQInstanceConfig> localConfig) {
            _localConfig = std::move(localConfig);
        }

        // if leaderPos == -1, current node is leader
        bool startCluster(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& nodesExceptThis, int leaderPos, int targetGroupId) {
            std::vector<braft::PeerId> peers(nodesExceptThis.size() + 1);
            // allocate peers[0]
            if (!PeerIdFromConfig(_localConfig->addr(), _localConfig->port, targetGroupId, peers[0])) {
                return false;
            }
            // allocate peers[i]
            for (int i=1; i<(int)nodesExceptThis.size(); i++) {
                if (!PeerIdFromConfig(nodesExceptThis[i-1]->addr(), nodesExceptThis[i-1]->port, targetGroupId, peers[i])) {
                    return false;
                }
            }
            FollowerRaftFSM* fsm = nullptr;
            if (leaderPos != -1) {  // current node is not leader
                fsm = new FollowerRaftFSM(peers[0], peers[leaderPos+1], _multiRaft);
            }
            if (_multiRaft->start(peers, 0, fsm) != 0) {
                return false;
            }
            return true;
        }

    protected:
        static bool PeerIdFromConfig(const std::string& ip, int port, int targetGroupId, braft::PeerId& pc) {
            if (!butil::str2ip(ip.data(), &pc.addr.ip)) {
                return false;
            }
            pc.addr.port = port;
            pc.idx = targetGroupId;
            return true;
        }

    private:
        std::shared_ptr<util::ZMQInstanceConfig> _localConfig;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaft;
    };
}