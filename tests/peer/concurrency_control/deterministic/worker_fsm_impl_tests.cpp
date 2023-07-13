//
// Created by peng on 2/21/23.
//

#include "peer/concurrency_control/deterministic/worker_fsm_impl.h"
#include "tests/transaction_utils.h"
#include "bthread/countdown_event.h"

#include "gtest/gtest.h"
#include "glog/logging.h"
#include "common/property.h"

class WorkerImplTest : public ::testing::Test {
protected:
    void SetUp() override {
        CHECK(util::Properties::LoadProperties());
        util::Properties::GetProperties()->getChaincodeProperties().install("transfer");
    };

    void TearDown() override {
    };

};


// please use peer::chaincode::SimpleTransfer chaincode
TEST_F(WorkerImplTest, TestSignalSendReceive) {
    constexpr int recordCount = 1000;
    constexpr int txnCount = 1000;
    // setup fsm
    auto fsm = std::make_shared<peer::cc::WorkerFSMImpl>();
    std::shared_ptr<peer::db::DBConnection> dbc = peer::db::DBConnection::NewConnection("testDB");
    CHECK(dbc != nullptr) << "create db failed!";
    fsm->setDB(dbc);
    // init db
    dbc->syncWriteBatch([](peer::db::DBConnection::WriteBatch* batch) {
        for (int i=0; i<recordCount; i++) {
            batch->Put(std::to_string(i), "0");
        }
        return true;
    });
    // set table
    auto table = std::make_shared<peer::cc::ReserveTable>();
    fsm->setReserveTable(table);

    // setup worker
    auto worker = peer::cc::Worker::NewWorker(fsm);
    ASSERT_TRUE(fsm != nullptr && worker != nullptr);

    bthread::CountdownEvent cd(1);
    worker->setCommandCallback([&](peer::cc::ReceiverState) {
        cd.signal(1);
    });

    // start worker
    ASSERT_TRUE(worker->checkAndStartService());
    worker->execute(peer::cc::InvokerCommand::START);
    cd.wait(); cd.reset(1);
    for (int i=0; i< 100; i++) {
        // setup transaction
        auto& txnList = fsm->getMutableTxnList();
        tests::TransactionUtils::CreateMockTxn(&txnList, txnCount, recordCount);
        // create mock transactions
        worker->execute(peer::cc::InvokerCommand::EXEC);
        cd.wait(); cd.reset(1);

        worker->execute(peer::cc::InvokerCommand::COMMIT);
        cd.wait(); cd.reset(1);
        // cleanup
        table->reset();
    }
    worker->execute(peer::cc::InvokerCommand::EXIT);
    cd.wait(); cd.reset(1);
    int totalValue = 0;
    for (int i=0; i<recordCount; i++) {
        std::string value;
        dbc->get(std::to_string(i), &value);
        totalValue += std::atoi(value.data());
    }
    ASSERT_TRUE(totalValue == 0);
}