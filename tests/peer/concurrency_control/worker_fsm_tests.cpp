//
// Created by peng on 2/20/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "bthread/countdown_event.h"
#include "peer/concurrency_control/worker_fsm.h"

class WorkerTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

};

class MockFSM: public peer::cc::WorkerFSM {
public:
    peer::cc::ReceiverState OnCreate() override {
        state = peer::cc::ReceiverState::READY;
        return state;
    }

    peer::cc::ReceiverState OnDestroy() override {
        state = peer::cc::ReceiverState::EXITED;
        return state;

    }
    peer::cc::ReceiverState OnExecuteTransaction() override {
        state = peer::cc::ReceiverState::FINISH_EXEC;
        return state;

    }
    peer::cc::ReceiverState OnCommitTransaction() override {
        state = peer::cc::ReceiverState::FINISH_COMMIT;
        return state;

    }

    peer::cc::ReceiverState state{};
};


TEST_F(WorkerTest, TestSignalSendReceive) {
    auto fsm = std::make_shared<MockFSM>();
    auto worker = peer::cc::Worker::NewWorker(fsm);
    ASSERT_TRUE(fsm != nullptr && worker != nullptr);
    // setup worker
    bthread::CountdownEvent cd(1);
    std::atomic<peer::cc::ReceiverState> state{};
    worker->setCommandCallback([&](peer::cc::ReceiverState ret) {
        state.store(ret, std::memory_order_release);
        cd.signal(1);
    });
    ASSERT_TRUE(worker->checkAndStartService());
    // emit signal
    worker->execute(peer::cc::InvokerCommand::START);
    cd.wait(); cd.reset(1);
    ASSERT_TRUE(state == peer::cc::ReceiverState::READY);
    for (int i=0; i< 100; i++) {
        worker->execute(peer::cc::InvokerCommand::EXEC);
        cd.wait(); cd.reset(1);
        ASSERT_TRUE(state == peer::cc::ReceiverState::FINISH_EXEC);

        worker->execute(peer::cc::InvokerCommand::COMMIT);
        cd.wait(); cd.reset(1);
        ASSERT_TRUE(state == peer::cc::ReceiverState::FINISH_COMMIT);

        // commit twice, for write based workload
        worker->execute(peer::cc::InvokerCommand::COMMIT);
        cd.wait(); cd.reset(1);
        ASSERT_TRUE(state == peer::cc::ReceiverState::FINISH_COMMIT);
    }
    worker->execute(peer::cc::InvokerCommand::CUSTOM);
    cd.wait(); cd.reset(1);
    ASSERT_TRUE(state == peer::cc::ReceiverState::FINISH_CUSTOM);
    worker->execute(peer::cc::InvokerCommand::EXIT);
    cd.wait(); cd.reset(1);
    ASSERT_TRUE(state == peer::cc::ReceiverState::EXITED);
}