//
// Created by user on 23-4-5.
//

#pragma once

#include "peer/consensus/block_order/interchain_order_manager.h"
#include "peer/consensus/block_order/agreement_raft_fsm.h"

#include "common/meta_rpc_server.h"
#include "common/property.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "proto/block_order.h"

namespace peer::consensus {
    class AsyncAgreementCallback {
    public:
        virtual ~AsyncAgreementCallback() = default;

        explicit AsyncAgreementCallback() {
            onBroadcastHandle = [this](const std::string& decision) { return applyRawBlockOrder(decision); };
        }

        // Called by raft fsm in the first RPC (Receive but may not be replicated in most region)
        inline auto onValidate(const proto::SignedBlockOrder& sb) { return validateSignatureOfBlockOrder(sb); }

        // Called on return after determining the final order of sub chain blocks
        inline auto onDeliver(int subChainId, int blockId) { return onDeliverHandle(subChainId, blockId); }

        // Called after receiving a message from raft, responsible for broadcasting to all local nodes
        inline auto onBroadcast(std::string decision) { return onBroadcastHandle(std::move(decision)); }

        // Called when the remote leader is down
        inline void onError(int subChainId) {
            // the group is down, invalid all the block
            // _orderManager->invalidateChain(subChainId);
            proto::BlockOrder bo {
                    .chainId = subChainId,
                    .blockId = -1,
                    .voteChainId = -1,
                    .voteBlockId = -1
            };
            ::proto::SignedBlockOrder sb;
            if (!bo.serializeToString(&sb.serializedBlockOrder)) {
                return;
            }
            std::string buffer;
            sb.serializeToString(&buffer);
            // broadcast to all nodes in this group
            onBroadcastHandle(std::move(buffer));
        }

        void init(int groupCount) {
            auto om = std::make_unique<v2::InterChainOrderManager>();
            om->setSubChainCount(groupCount);
            om->setDeliverCallback([this](const v2::InterChainOrderManager::Cell* c) {
                // return the final decision to caller
                this->onDeliver(c->subChainId, c->blockNumber);
            });
            // Optimize-1: order next block as soon as receiving the previous block
            // for (int i=0; i<groupCount; i++) {
            //     for (int j=0; j<groupCount; j++) {
            //         CHECK(om->pushDecision(i, 0, { j, -1 }));
            //     }
            // }
            _orderManager = std::move(om);
            // all handles are set
            CHECK(onDeliverHandle && onBroadcastHandle);
        }

    protected:
        std::function<bool(int, int)> onDeliverHandle;

        std::function<bool(std::string decision)> onBroadcastHandle;

    public:
        bool applyRawBlockOrder(const std::string& decision) {
            proto::SignedBlockOrder sb;
            if (!sb.deserializeFromString(decision)) {
                return false;
            }
            if (!onValidate(sb)) {
                return false;
            }
            proto::BlockOrder bo{};
            if (!bo.deserializeFromString(sb.serializedBlockOrder)) {
                return false;
            }
            if (bo.voteChainId == -1) {   // this is an error message
                CHECK(bo.blockId == -1 && bo.voteBlockId == -1);
                // the group is down, invalid all the block
                return _orderManager->invalidateChain(bo.chainId);
            }
            return _orderManager->pushDecision(bo.chainId, bo.blockId, { bo.voteChainId, bo.voteBlockId });
            // Optimize-1: order next block as soon as receiving the previous block
            // return _orderManager->pushDecision(bo.chainId, bo.blockId + 1, { bo.voteChainId, bo.voteBlockId + 1 });
        }

    protected:
        std::unique_ptr<v2::InterChainOrderManager> _orderManager;
        // BCCSP and thread pool
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;

    public:
        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _threadPoolForBCCSP = std::move(threadPool);
        }

        bool validateSignatureOfBlockOrder(const proto::SignedBlockOrder& sb) {
            if (sb.signatures.empty()) {    // optimize
                // DLOG(WARNING) << "Sigs are empty in validateSignatureOfBlockOrder!";
                return true;
            }
            bool success = true;
            auto numRoutines = (int)_threadPoolForBCCSP->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPoolForBCCSP->push_emergency_task([&, start=i] {
                    auto& payload = sb.serializedBlockOrder;
                    for (int j = start; j < (int)sb.signatures.size(); j += numRoutines) {
                        auto& signature = sb.signatures[j];
                        const auto key = _bccsp->GetKey(signature.ski);
                        if (key == nullptr) {
                            LOG(WARNING) << "Can not load key, ski: " << signature.ski;
                            success = false;
                            break;
                        }
                        if (!key->Verify(signature.digest, payload.data(), payload.size())) {
                            success = false;
                            break;
                        }
                    }
                    countdown.signal();
                });
            }
            countdown.wait();
            return success;
        }
    };

    // The cluster orders the blocks locally(with bft) and then broadcasts to other clusters(with raft)
    // Meanwhile, the cluster receives the ordering results of other clusters(with raft)
    // Generate a final block order based on the aggregation of all results
    class AsyncAgreement {
    private:
        std::shared_ptr<util::ZMQInstanceConfig> _localConfig;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaft;
        std::unique_ptr<v2::OrderAssigner> _localOrderAssigner;
        std::shared_ptr<AsyncAgreementCallback> _callback;
        braft::PeerId _localPeerId;

    public:
        // groupCount: sub chain ids are from 0 to groupCount-1
        static std::unique_ptr<AsyncAgreement> NewAsyncAgreement(std::shared_ptr<util::ZMQInstanceConfig> localConfig,
                                                                 std::shared_ptr<AsyncAgreementCallback> callback) {
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