//
// Created by peng on 2/21/23.
//

#include "gtest/gtest.h"
#include "glog/logging.h"

#include <random>

#include "bthread/countdown_event.h"
#include "peer/concurrency_control/deterministic/worker_fsm_impl.h"

class WorkerImplTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

    static void CreateMockTxn(std::vector<std::unique_ptr<proto::Transaction>>* txnList, int count, int range) {
        txnList->clear();
        txnList->reserve(count);
        auto envelopList = CreateMockEnvelop(count, range);
        for(int i=0; i<count; i++) {
            auto txn = proto::Transaction::NewTransactionFromEnvelop(std::move(envelopList[i]));
            txnList->push_back(std::move(txn));
        }
    }

    static std::string ParamToString(const std::vector<std::string>& args) {
        std::string argRaw;
        zpp::bits::out out(argRaw);
        CHECK(!failure(out(args)));
        return argRaw;
    }

    static std::vector<std::unique_ptr<proto::Envelop>> CreateMockEnvelop(int count, int range) {
        // init random
        std::random_device rd;
        std::default_random_engine rng(rd());
        std::uniform_int_distribution<> dist(0, range-1);

        std::vector<std::unique_ptr<proto::Envelop>> envelopList;
        envelopList.reserve(count);
        for (int i=0; i<count; i++) {
            proto::UserRequest request;
            // set from and to
            request.setArgs(ParamToString({std::to_string(dist(rng)), std::to_string(dist(rng))}));
            // serialize
            std::string requestRaw;
            zpp::bits::out out(requestRaw);
            CHECK(!failure(out(request)));
            // wrap it
            std::unique_ptr<proto::Envelop> envelop(new proto::Envelop);
            envelop->setPayload(std::move(requestRaw));
            // compute tid
            proto::SignatureString signature;
            auto digest = std::to_string(i);
            std::copy(digest.begin(), digest.end(), signature.digest.data());
            envelop->setSignature(std::move(signature));
            envelopList.push_back(std::move(envelop));
        }
        return envelopList;
    }

};


// please use peer::chaincode::SimpleTransfer chaincode
TEST_F(WorkerImplTest, TestSignalSendReceive) {
    constexpr int recordCount = 1000;
    constexpr int txnCount = 1000;
    // setup fsm
    auto fsm = std::make_shared<peer::cc::WorkerFSMImpl>();
    fsm->setId(1);
    std::shared_ptr<peer::db::LeveldbConnection> dbc = peer::db::LeveldbConnection::NewLeveldbConnection("testDB");
    CHECK(dbc != nullptr) << "create db failed!";
    fsm->setDB(dbc);
    // init db
    dbc->syncWriteBatch([](leveldb::WriteBatch* batch){
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
        CreateMockTxn(&txnList, txnCount, recordCount);
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