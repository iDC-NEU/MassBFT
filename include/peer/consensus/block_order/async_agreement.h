//
// Created by user on 23-4-5.
//

#pragma once

#include "peer/consensus/block_order/interchain_order_manager.h"
#include "peer/consensus/block_order/agreement_raft_fsm.h"
#include "peer/consensus/block_order/local_distributor.h"

#include "common/meta_rpc_server.h"
#include "common/property.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "proto/block_order.h"

namespace peer::consensus {
    // The cluster orders the blocks locally(with bft) and then broadcasts to other clusters(with raft)
    // Meanwhile, the cluster receives the ordering results of other clusters(with raft)
    // Generate a final block order based on the aggregation of all results
    class AsyncAgreement {
    private:
        std::shared_ptr<util::ZMQInstanceConfig> _localConfig;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaft;
        std::unique_ptr<v2::OrderAssigner> _localOrderAssigner;
        std::shared_ptr<v2::RaftCallback> _callback;
        braft::PeerId _localPeerId;

    public:
        // groupCount: sub chain ids are from 0 to groupCount-1
        static std::unique_ptr<AsyncAgreement> NewAsyncAgreement(std::shared_ptr<util::ZMQInstanceConfig> localConfig,
                                                                 std::shared_ptr<v2::RaftCallback> callback) {
            auto aa = std::make_unique<AsyncAgreement>();
            aa->_localConfig = std::move(localConfig);
            auto& cfg = aa->_localConfig;
            if (!PeerIdFromConfig(cfg->pubAddr(), cfg->port, cfg->nodeConfig->groupId, aa->_localPeerId)) {
                return nullptr;
            }
            aa->_callback = std::move(callback);
            aa->_multiRaft = std::make_unique<util::raft::MultiRaftFSM>("blk_order_cluster");
            aa->_localOrderAssigner = std::make_unique<v2::OrderAssigner>();
            aa->_localOrderAssigner->setLocalChainId(cfg->nodeConfig->groupId);
            // start local rpc instance
            if (util::DefaultRpcServer::AddRaftService(cfg->port) != 0) {
                return nullptr;
            }
            if (util::DefaultRpcServer::Start(cfg->port) != 0) {
                return nullptr;
            }
            // TODO: CONNECT TO OTHER PEERS IN LOCAL CLUSTER
            return aa;
        }

        virtual ~AsyncAgreement() {
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
            fsm->setOnApplyCallback([this](auto&& data) {
                auto dataStr = data.to_string();
                return _callback->onBroadcast(std::move(dataStr));
            });

            fsm->setOnErrorCallback([this](const braft::PeerId& peerId, const int64_t& term, const butil::Status& status) {
                LOG(ERROR) << "Remote leader error: " << status << ", LeaderId: " << peerId << ", term: " << term;
                return _callback->onError(peerId.idx);
            });

            if (_multiRaft->start(peers, myIdIndex, fsm) != 0) {
                return false;
            }
            return true;
        }

    private:
        std::mutex leaderVotingMutex;

    public:
        // TODO: pipeline the requests
        // the instance MUST BE the leader of local group
        bool onLeaderVotingNewBlock(int chainId, int blockId) {
            std::unique_lock guard(leaderVotingMutex);
            auto localVC = _localOrderAssigner->getBlockOrder(chainId, blockId);
            proto::BlockOrder bo {
                    .chainId = chainId,
                    .blockId = blockId,
                    .voteChainId = localVC.first,
                    .voteBlockId = localVC.second
            };
            // TODO: use local BFT consensus to consensus the bo
            //  BFT(bo) -> true
            ::proto::SignedBlockOrder sb;
            if (!bo.serializeToString(&sb.serializedBlockOrder)) {
                return false;
            }
            std::string buffer;
            sb.serializeToString(&buffer);
            return apply(buffer);
        }

        // the instance MUST BE the follower of local group
        // NOT thread safe, called seq by BFT instance
        bool onValidateVotingNewBlock(const proto::BlockOrder& bo) {
            auto localVC = _localOrderAssigner->getBlockOrder(bo.chainId, bo.blockId);
            if (localVC.first != bo.voteChainId) {
                return false;
            }
            if (localVC.second != bo.voteBlockId) {
                return false;
            }
            return true;
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
            // TODO: use complex task.done
            util::raft::NullOptionClosure done;
            braft::Task task;
            task.data = &data;
            task.done = &done;
            leader->apply(task);
            if (!task.done->status().ok()) {
                LOG(WARNING) << "Can not apply task, " << task.done->status().error_cstr();
                return false;
            }
            return true;
        }
    };
}