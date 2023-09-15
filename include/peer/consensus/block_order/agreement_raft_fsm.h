//
// Created by user on 23-9-14.
//

#pragma once

#include "common/raft/multi_raft_fsm.h"
#include "peer/consensus/block_order/local_distributor.h"

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
            while (status == (int)Status::INIT) {
                auto timeSpec = butil::milliseconds_from_now(1000);
                bthread::butex_wait(_running, status, &timeSpec);
                status = _running->load(std::memory_order_relaxed);
                LOG(INFO) << "Waiting leader to be ready: " << _leaderId << ", status: " << status;
            }
            _running->store((int)Status::READY);
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
                DLOG(INFO) << "Transfer leader to: " << _leaderId;
                while (_multiRaftFsm->find_node(_myId)->transfer_leadership_to(_leaderId) != 0) {
                    LOG(ERROR) << "Transfer leader failed: (" << _myId << " => " << _leaderId << ")";
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            } else {
                DLOG(INFO) << "I am the expected leader, " << _leaderId;
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
                LOG(ERROR) << "Remote leader error: " << ctx.status() << ", LeaderId: " << ctx.leader_id() << ", term: " << ctx.term();
                _callback->onError(ctx.leader_id().idx);  // group peerId.idx is down
                return;
            }
        }

        void on_start_following(const ::braft::LeaderChangeContext& ctx) override {
            if (ctx.leader_id() == _leaderId) {
                DLOG(INFO) << "Start following remote leader, " << _leaderId;
                _running->store((int)Status::READY);
                bthread::butex_wake_all(_running);
            }
        }

        void on_apply(::braft::Iterator& iter) override {
            for (; iter.valid(); iter.next()) {
                if (!_callback->onBroadcast(iter.data().to_string())) {
                    LOG(ERROR) << "addr " << get_address()  << " apply " << iter.index()
                               << " data_size " << iter.data().size() << " failed!";
                    _callback->onError(_leaderId.idx);  // group peerId.idx is down
                    return;
                }
            }
        }

        bool on_follower_receive(int term, int index, const ::butil::IOBuf& data) override {
            if (is_leader()) {  // skip leader
                return true;
            }
            // DLOG(INFO) << "Follower receive index: " << index << " at term: " << term << ", data size: " << data.size();
            return _callback->onValidate(data.to_string());    // accept it
        }

    public:
        void setCallback(auto&& callback) { _callback = std::forward<decltype(callback)>(callback); }

    private:
        // if running == 0, the raft instance is not ready
        // if running == 1, the raft instance is functional
        // if running == -1, the raft instance must be destroyed
        butil::atomic<int>* _running;
        braft::PeerId _myId;
        braft::PeerId _leaderId;
        std::shared_ptr<util::raft::MultiRaftFSM> _multiRaftFsm;
        std::shared_ptr<v2::RaftCallback> _callback;
    };
}
