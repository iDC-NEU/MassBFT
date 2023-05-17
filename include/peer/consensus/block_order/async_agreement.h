//
// Created by user on 23-4-5.
//

#pragma once

#include "peer/consensus/block_order/interchain_order_manager.h"

#include "common/raft/multi_raft_fsm.h"
#include "common/meta_rpc_server.h"
#include "common/property.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "proto/block_order.h"

namespace peer::consensus {

    class AgreementRaftFSM: public util::raft::SingleRaftFSM {
    public:
        enum class Status {
            INIT = 0,
            READY = 1,
            ERROR = -1,
        };

        AgreementRaftFSM(auto&& myId, auto&& leaderId, auto&& fsm) {
            _running = bthread::butex_create_checked<butil::atomic<int>>();
            _running->store((int)Status::INIT, std::memory_order_relaxed);
            _myId = std::forward<decltype(myId)>(myId);
            _leaderId = std::forward<decltype(leaderId)>(leaderId);
            _multiRaftFsm = std::forward<decltype(fsm)>(fsm);
        }

        ~AgreementRaftFSM() override {
            bthread::butex_destroy(_running);
        }

        bool ready() const {
            auto status = _running->load(std::memory_order_relaxed);
            auto timeSpec = butil::milliseconds_from_now(1000);
            while (status == (int)Status::INIT) {
                bthread::butex_wait(_running, status, &timeSpec);
                status = _running->load(std::memory_order_relaxed);
                LOG(INFO) << "Waiting leader to be ready: " << _leaderId;
            }
            return _myId == _leaderId;
        }

    protected:
        void on_leader_start(int64_t term) override {
            util::raft::SingleRaftFSM::on_leader_start(term);
            if (_myId != _leaderId) {
                auto status = _running->load(std::memory_order_relaxed);
                if (status == (int)Status::ERROR) {
                    // TODO: stop this raft group
                    return; // raft group is stop, return
                }
                LOG(INFO) << "Transfer leader to: " << _leaderId;
                _multiRaftFsm->find_node(_myId)->transfer_leadership_to(_leaderId);
            } else {
                LOG(INFO) << "I am the expected leader, " << _leaderId;
                _running->store((int)Status::READY);
                bthread::butex_wake_all(_running);
            };
        }

        void on_stop_following(const ::braft::LeaderChangeContext& ctx) override {
            if (ctx.leader_id() == _leaderId) {
                // emit a view change
                LOG(ERROR) << "Remote leader contains error, " << _leaderId;
                _running->store((int)Status::ERROR);
                bthread::butex_wake_all(_running);
            }
        }

        void on_start_following(const ::braft::LeaderChangeContext& ctx) override {
            if (ctx.leader_id() == _leaderId) {
                LOG(INFO) << "Start following remote leader, " << _leaderId;
                _running->store((int)Status::READY);
                bthread::butex_wake_all(_running);
            }
        }

        void on_apply(::braft::Iterator& iter) override {
            for (; iter.valid(); iter.next()) {
                if (!_onApplyCallback || !_onApplyCallback(iter.data())) {
                    LOG(ERROR) << "addr " << get_address()  << " apply " << iter.index()
                               << " data_size " << iter.data().size() << " failed!";
                }
            }
        }

    public:
        void setOnApplyCallback(auto&& callback) { _onApplyCallback = std::forward<decltype(callback)>(callback); }

    private:
        // if running == 0, the raft instance is not ready
        // if running == 1, the raft instance is functional
        // if running == -1, the raft instance must be destroyed
        butil::atomic<int>* _running;
        braft::PeerId _myId;
        braft::PeerId _leaderId;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaftFsm;
        std::function<bool(const butil::IOBuf& data)> _onApplyCallback;
    };

    class AsyncAgreementCallback {
    public:
        virtual ~AsyncAgreementCallback() = default;

        explicit AsyncAgreementCallback() {
            onValidateHandle = [this](const proto::SignedBlockOrder& sb) { return validateSignatureOfBlockOrder(sb); };
            onBroadcastHandle = [this](const std::string& decision) { return applyRawBlockOrder(decision); };
        }

        // Called by raft fsm in the first RPC (Receive but may not be replicated in most region)
        inline auto onValidate(const proto::SignedBlockOrder& sb) { return onValidateHandle(sb); }

        // Called on return after determining the final order of sub chain blocks
        inline auto onDeliver(int subChainId, int blockId) { return onDeliverHandle(subChainId, blockId); }

        // Called after receiving a message from raft, responsible for broadcasting to all local nodes
        inline auto onBroadcast(std::string decision) { return onBroadcastHandle(std::move(decision)); }

        void init(int groupCount) {
            auto om = std::make_unique<v2::InterChainOrderManager>();
            om->setSubChainCount(groupCount);
            om->setDeliverCallback([this](const v2::InterChainOrderManager::Cell* c) {
                // return the final decision to caller
                this->onDeliver(c->subChainId, c->blockNumber);
            });
            _orderManager = std::move(om);
            // all handles are set
            CHECK(onValidateHandle && onDeliverHandle && onBroadcastHandle);
        }

    protected:
        std::function<bool(const proto::SignedBlockOrder& sb)> onValidateHandle;

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
            return _orderManager->pushDecision(bo.chainId, bo.blockId, { bo.voteChainId, bo.voteBlockId });
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
            fsm->setOnApplyCallback([this](auto&& data) { return this->onApply(std::forward<decltype(data)>(data)); });
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
            apply(buffer);
            return true;
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

        // thread safe, called by raft fsm (followers and the leader)
        bool onApply(const butil::IOBuf& data) {
            auto dataStr = data.to_string();
            return _callback->onBroadcast(std::move(dataStr));
        };
    };
}