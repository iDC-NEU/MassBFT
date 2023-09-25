//
// Created by user on 23-9-14.
//

#pragma once

#include "peer/consensus/block_order/agreement_raft_fsm.h"

#include "common/meta_rpc_server.h"
#include "common/property.h"
#include "zpp_bits.h"

namespace peer::consensus::rb {
    class RoundBasedAgreement {
    private:
        std::shared_ptr<util::ZMQInstanceConfig> _localConfig;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaft;
        std::shared_ptr<v2::RaftCallback> _callback;
        braft::PeerId _localPeerId;

    public:
        static std::unique_ptr<RoundBasedAgreement> NewRoundBasedAgreement(
                std::shared_ptr<util::ZMQInstanceConfig> localConfig,
                std::shared_ptr<v2::RaftCallback> callback) {

            auto aa = std::make_unique<RoundBasedAgreement>();
            aa->_localConfig = std::move(localConfig);
            auto& cfg = aa->_localConfig;
            if (!PeerIdFromConfig(cfg->pubAddr(), cfg->port, cfg->nodeConfig->groupId, aa->_localPeerId)) {
                return nullptr;
            }
            aa->_callback = std::move(callback);
            aa->_multiRaft = std::make_unique<util::raft::MultiRaftFSM>("blk_order_cluster");
            // start local rpc instance
            if (util::DefaultRpcServer::AddRaftService(cfg->port) != 0) {
                return nullptr;
            }
            if (util::DefaultRpcServer::Start(cfg->port) != 0) {
                return nullptr;
            }
            return aa;
        }

        virtual ~RoundBasedAgreement() {
            if (_localConfig != nullptr) {
                util::DefaultRpcServer::Stop(_localConfig->port);
            }
        }

        // wait until I become the leader of the local raft group
        [[nodiscard]] bool ready() const {
            auto* fsm = dynamic_cast<AgreementRaftFSM*>(_multiRaft->find_fsm(_localPeerId));
            return fsm && fsm->ready();
        }

        // leaderPos: the leader of current cluster
        // targetGroupId: the group involved in
        bool startCluster(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& nodes, int leaderPos) {
            std::vector<braft::PeerId> peers(nodes.size());
            int myIdIndex = -1;
            for (int i=0; i<(int)nodes.size(); i++) {
                if (nodes[i] == _localConfig) {
                    myIdIndex = i;
                }
                // targetGroupId = the region id of leader, since each region has only one leader
                if (!PeerIdFromConfig(nodes[i]->pubAddr(), nodes[i]->port, nodes[leaderPos]->nodeConfig->groupId, peers[i])) {
                    return false;
                }
            }
            if (myIdIndex == -1) {
                return false; // can not find local index
            }
            auto* fsm = new AgreementRaftFSM(peers[myIdIndex], peers[leaderPos], _multiRaft);
            fsm->setCallback(_callback);
            if (_multiRaft->start(peers, myIdIndex, fsm) != 0) {
                return false;
            }
            return true;
        }

    private:
        std::mutex leaderVotingMutex;

    public:
        // the instance MUST BE the leader of local group
        bool onReplicatingNewBlock(int chainId, int blockId) {
            std::unique_lock guard(leaderVotingMutex);
            if (_localConfig->nodeConfig->groupId != chainId) {
                return true;    // only consensus block generate by this group
            }
            // LOG(INFO) << "Leader of group " << chainId << " start consensus block " << blockId;
            std::string buffer;
            zpp::bits::out out(buffer);
            if(failure(out(chainId, blockId))) {
                return false;
            }
            return apply(buffer);
        }

    public:
        // Generate a peer id using local id and target group id
        static bool PeerIdFromConfig(const std::string& ip, int port, int targetGroupId, braft::PeerId& pc) {
            if (butil::str2ip(ip.data(), &pc.addr.ip) != 0) {
                return false;
            }
            pc.addr.port = port;
            pc.idx = targetGroupId;
            return true;
        }

    protected:
        // thread safe, called by leader
        bool apply(std::string& content) {
            auto* leader = _multiRaft->find_node(_localPeerId);
            butil::IOBuf data;
            data.append(content);
            auto done = new util::raft::NullOptionClosure;
            braft::Task task;
            task.data = &data;
            task.done = done;
            leader->apply(task);
            return true;
        }
    };
}
