//
// Created by peng on 2/10/23.
//

#ifndef NBP_FSM_H
#define NBP_FSM_H

#include "braft/node.h"
#include "braft/enum.pb.h"
#include "braft/errno.pb.h"
#include "braft/snapshot_throttle.h"
#include "braft/snapshot_executor.h"

#include "raft_log.h"

namespace util::raft {
    // the fsm of a single raft instance
    class SingleRaftFSM : public ::braft::StateMachine {
    public:
        // address_: the local endpoint
        explicit SingleRaftFSM(const butil::EndPoint& address_)
                : address(address_)
                , logs()
                , applied_index(0)
                , snapshot_index(0)
                , _on_start_following_times(0)
                , _on_stop_following_times(0)
                , _leader_term(-1)
                , _on_leader_start_closure(nullptr) {

        }

        // The closure is invoked only once when the node become leader
        void set_on_leader_start_closure(::braft::Closure* closure) {
            _on_leader_start_closure = closure;
        }

        void on_leader_start(int64_t term) override {
            _leader_term = term;
            if (_on_leader_start_closure) {
                LOG(INFO) << "addr " << address << " before leader start closure";
                _on_leader_start_closure->Run();
                LOG(INFO) << "addr " << address << " after leader start closure";
                _on_leader_start_closure = nullptr;
            }
        }

        void on_leader_stop(const butil::Status&) override {
            _leader_term = -1;
        }

        [[nodiscard]] butil::EndPoint get_address() const { return address; }

        [[nodiscard]] bool is_leader() const { return _leader_term > 0; }

        [[nodiscard]] int64_t get_snapshot_index() const { return snapshot_index; }

        [[nodiscard]] const RaftLog<butil::IOBuf>& get_logs() const { return logs; }

        void on_apply(::braft::Iterator& iter) override {
            for (; iter.valid(); iter.next()) {
                LOG(INFO) << "addr " << address  << " apply " << iter.index() << " data_size " << iter.data().size();
                DLOG(INFO) << "data " << iter.data();
                ::braft::AsyncClosureGuard closure_guard(iter.done());
                logs.write([&](auto* raw){
                    raw->push_back(iter.data());
                });
                applied_index = iter.index();
            }
        }

        void on_shutdown() override {
            LOG(INFO) << "addr " << address << " is down";
        }

        void on_snapshot_save(::braft::SnapshotWriter* writer, ::braft::Closure* done) override {
            std::string file_path = writer->get_path();
            file_path.append("/data");
            brpc::ClosureGuard done_guard(done);

            LOG(INFO) << "on_snapshot_save to " << file_path;

            int fd = ::creat(file_path.c_str(), 0644);
            if (fd < 0) {
                LOG(ERROR) << "create file failed, path: " << file_path << " err: " << berror();
                done->status().set_error(EIO, "Fail to create file");
                return;
            }
            // write snapshot and log to file
            logs.read([&](const auto* raw){
                for (auto data : *raw) {
                    auto len = data.size();
                    auto ret = write(fd, &len, sizeof(size_t));
                    CHECK_EQ(ret, sizeof(size_t));
                    data.cut_into_file_descriptor(fd, len);
                }
                ::close(fd);
                snapshot_index = applied_index;
            });
            writer->add_file("data");
        }

        int on_snapshot_load(::braft::SnapshotReader* reader) override {
            std::string file_path = reader->get_path();
            file_path.append("/data");

            LOG(INFO) << "on_snapshot_load from " << file_path;

            int fd = ::open(file_path.c_str(), O_RDONLY);
            if (fd < 0) {
                LOG(ERROR) << "creat file failed, path: " << file_path << " err: " << berror();
                return EIO;
            }
            logs.write([&](auto* raw){
                while (true) {
                    size_t len = 0;
                    auto ret = read(fd, &len, sizeof(size_t));
                    if (ret <= 0) {
                        break;
                    }
                    butil::IOPortal data;
                    data.append_from_file_descriptor(fd, len);
                    raw->push_back(data);
                }
                ::close(fd);
            });
            return 0;
        }

        void on_start_following(const ::braft::LeaderChangeContext& start_following_context) override {
            LOG(INFO) << "address " << address << " start following new leader: "
                      <<  start_following_context;
            ++_on_start_following_times;
        }

        void on_stop_following(const ::braft::LeaderChangeContext& stop_following_context) override {
            LOG(INFO) << "address " << address << " stop following old leader: "
                      <<  stop_following_context;
            ++_on_stop_following_times;
        }

        void on_configuration_committed(const ::braft::Configuration& conf, int64_t index) override {
            LOG(INFO) << "address " << address << " commit conf: " << conf << " at index " << index;
        }

    private:
        butil::EndPoint address;
        RaftLog<butil::IOBuf> logs;
        int64_t applied_index;
        int64_t snapshot_index;
        int64_t _on_start_following_times;
        int64_t _on_stop_following_times;
        volatile int64_t _leader_term;
        ::braft::Closure* _on_leader_start_closure;
    };

}
#endif //NBP_FSM_H
