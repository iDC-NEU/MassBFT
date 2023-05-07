//
// Created by user on 23-4-5.
//

#pragma once

#include "peer/consensus/raft/interchain_order_manager.h"

#include "common/raft/multi_raft_fsm.h"
#include "common/property.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "proto/block_order.h"

namespace peer::consensus {

    class AgreementRaftFSM: public util::raft::SingleRaftFSM {
    public:
        AgreementRaftFSM(auto&& myId, auto&& leaderId, auto&& fsm) {
            _myId = std::forward<decltype(myId)>(myId);
            _leaderId = std::forward<decltype(leaderId)>(leaderId);
            _multiRaftFsm = std::forward<decltype(fsm)>(fsm);
        }

    protected:
        void on_leader_start(int64_t term) override {
            util::raft::SingleRaftFSM::on_leader_start(term);
            if (init && _myId != _leaderId) {
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

        void on_apply(::braft::Iterator& iter) override {
            for (; iter.valid(); iter.next()) {
                LOG(INFO) << "addr " << get_address()  << " apply " << iter.index() << " data_size " << iter.data().size();
                if (!onApplyCallback || !onApplyCallback(iter.data())) {
                    LOG(ERROR) << "Apply entrance failed!";
                }
            }
        }

    public:
        void setOnApplyCallback(auto&& callback) { onApplyCallback = std::forward<decltype(callback)>(callback); }

    private:
        bool init = true;
        braft::PeerId _myId;
        braft::PeerId _leaderId;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaftFsm;
        std::function<bool(const butil::IOBuf& data)> onApplyCallback;
    };

    // The cluster orders the blocks locally(with bft) and then broadcasts to other clusters(with raft)
    // Meanwhile, the cluster receives the ordering results of other clusters(with raft)
    // Generate a final block order based on the aggregation of all results
    class AsyncAgreement {
    private:
        std::shared_ptr<util::ZMQInstanceConfig> _localConfig;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaft;
        std::unique_ptr<v2::OrderAssigner> _localOrderAssigner;
        std::unique_ptr<v2::InterChainOrderManager> _orderManager;

    public:
        static std::unique_ptr<AsyncAgreement> NewAsyncAgreement(std::shared_ptr<util::ZMQInstanceConfig> localConfig,
                                                                 int groupCount) {
            auto aa = std::make_unique<AsyncAgreement>();
            aa->_localConfig = std::move(localConfig);
            aa->_multiRaft = std::make_unique<util::raft::MultiRaftFSM>("blk_order_cluster");
            aa->_localOrderAssigner = std::make_unique<v2::OrderAssigner>();
            aa->_orderManager = std::make_unique<v2::InterChainOrderManager>();
            aa->_orderManager->setSubChainCount(groupCount);
            aa->_orderManager->setDeliverCallback([ptr = aa.get()](const v2::InterChainOrderManager::Cell* c) {
                // return the final decision to caller
                ptr->onApplyCallback(c->subChainId, c->blockNumber);
            });
            // TODO: CONNECT TO OTHER PEERS IN LOCAL CLUSTER
            return aa;
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
            auto* fsm = new AgreementRaftFSM(peers[0], peers[leaderPos+1], _multiRaft);
            fsm->setOnApplyCallback([this](auto&& data) { return this->onApply(std::forward<decltype(data)>(data)); });
            if (_multiRaft->start(peers, 0, fsm) != 0) {
                return false;
            }
            return true;
        }

    private:
        std::mutex leaderVotingMutex;

    public:
        // TODO: pipeline the requests
        // the instance MUST BE the leader of local group
        bool OnLeaderVotingNewBlock(int chainId, int blockId) {
            std::unique_lock guard(leaderVotingMutex);
            CHECK(chainId == _localConfig->nodeConfig->groupId);
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
            apply(buffer);
            return true;
        }

        // the instance MUST BE the follower of local group
        // NOT thread safe, called seq by BFT instance
        bool OnValidateVotingNewBlock(const proto::BlockOrder& bo) {
            auto localVC = _localOrderAssigner->getBlockOrder(bo.chainId, bo.blockId);
            if (localVC.first != bo.voteChainId) {
                return false;
            }
            if (localVC.second != bo.voteBlockId) {
                return false;
            }
            return true;
        }

    protected:
        // Generate a peer id using local id and target group id
        static bool PeerIdFromConfig(const std::string& ip, int port, int targetGroupId, braft::PeerId& pc) {
            if (!butil::str2ip(ip.data(), &pc.addr.ip)) {
                return false;
            }
            pc.addr.port = port;
            pc.idx = targetGroupId;
            return true;
        }

        // thread safe
        bool apply(std::string& content) {
            braft::PeerId pc;
            if (!PeerIdFromConfig(_localConfig->addr(), _localConfig->port, _localConfig->nodeConfig->groupId, pc)) {
                return false;
            }
            if (!_multiRaft->find_fsm(pc)->is_leader()) {
                LOG(WARNING) << "leader is not ready!";
                return false;
            }
            auto* leader = _multiRaft->find_node(pc);
            butil::IOBuf data;
            data.append(content);
            braft::Task task;
            task.data = &data;
            // TODO: FILL IN task.done
            leader->apply(task);
            return true;
        }

    private:
        std::function<bool(int chainId, int blockNumber)> onApplyCallback;
        // BCCSP and thread pool
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;

    public:
        void setOnApplyCallback(auto&& callback) { onApplyCallback = std::forward<decltype(callback)>(callback); }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _threadPoolForBCCSP = std::move(threadPool);
        }

    protected:
        // this function is called by raft fsm, thread safe
        bool onApply(const butil::IOBuf& data) {
            proto::SignedBlockOrder sb;
            if (sb.deserializeFromString(data.to_string())) {
                return false;
            }
            if (!validateSignatureOfBlockOrder(sb)) {
                return false;
            }
            proto::BlockOrder bo{};
            if (bo.deserializeFromString(sb.serializedBlockOrder)) {
                return false;
            }
            // TODO: broadcast the decision in local cluster(avoiding a byzantine leader)
            // it is a valid bo, can safely push decision
            return _orderManager->pushDecision(bo.chainId, bo.blockId, { bo.voteChainId, bo.voteBlockId });
        };

        bool validateSignatureOfBlockOrder(const proto::SignedBlockOrder& sb) {
            if (sb.signatures.empty()) {    // optimize
                DLOG(WARNING) << "Sigs are empty in validateSignatureOfBlockOrder!";
                return true;
            }
            bool success = true;
            auto numRoutines = (int)_threadPoolForBCCSP->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPoolForBCCSP->push_task([&, start=i] {
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
}