//
// Created by peng on 2/10/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include "common/raft/raft_fsm.h"
#include "common/raft/node_closure.h"


class FSMTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

// TODO: test the cluster
TEST_F(FSMTest, IntrgrateTest) {
    util::raft::SingleRaftFSM fsm;
}
