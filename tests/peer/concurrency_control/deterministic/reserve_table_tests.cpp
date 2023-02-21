//
// Created by peng on 2/19/23.
//

#include "peer/concurrency_control/deterministic/reserve_table.h"
#include "common/thread_pool_light.h"

#include "bthread/countdown_event.h"
#include <random>

#include "gtest/gtest.h"

class ReserveTableTest : public ::testing::Test {
protected:
    void SetUp() override {

    };

    void TearDown() override {

    };

    static auto CreateRandomKVs(int keySize=10, int range=10000) {
        std::random_device rd;
        std::default_random_engine rng(rd());
        std::uniform_int_distribution<> dist(0, range-1);

        proto::KVList reads;
        proto::KVList writes;
        for (int i=0; i<keySize; i++) {
            reads.push_back(std::make_unique<proto::KV>(std::to_string(dist(rng)), "value"));
            writes.push_back(std::make_unique<proto::KV>(std::to_string(dist(rng)), "value"));
        }
        return std::make_pair(std::move(reads), std::move(writes));
    }

    static auto testCase(util::thread_pool_light* tp=nullptr) {
        static constexpr int range = 100000;
        static constexpr int txnCnt = 5000;
        static constexpr int keySize = 2;   // >=2
        std::vector<int> moneyList(range);
        std::array<std::pair<proto::KVList, proto::KVList>, txnCnt> txnList;
        for (auto& txn: txnList) {
            txn = CreateRandomKVs(keySize, range);
        }
        peer::cc::ReserveTable table;
        std::array<std::shared_ptr<proto::tid_type>, txnCnt> tidList;
        for (uint i=0; i<(uint)tidList.size(); i++) {
            tidList[i] = std::make_shared<proto::tid_type>();
            auto ptr = reinterpret_cast<uint*>(tidList[i]->data());
            *ptr = i;
        }

        if (tp == nullptr) {
            for (int i=0; i<(int)txnList.size(); i++) {
                table.reserveRWSets(txnList[i].first, txnList[i].second, tidList[i]);
            }
        } else {
            auto tc = (int)tp->get_thread_count();
            bthread::CountdownEvent countdown(tc);
            for (int i=0; i<tc; i++) {
                tp->push_task([&, start=i]{
                    for (int j = start; j < (int)txnList.size(); j += tc) {
                        table.reserveRWSets(txnList[j].first, txnList[j].second, tidList[j]);
                    }
                    countdown.signal();
                });
            }
            countdown.wait();
        };

        int aborted = 0;
        for (int i=0; i<(int)txnList.size(); i++) {
            auto dep = table.analysisDependent(txnList[i].first, txnList[i].second, *tidList[i]);
            if (!dep.waw && (!dep.war || !dep.raw)) {    //  war / raw / no dependency, commit it.
                auto read = std::stoi(std::string(txnList[i].second[0]->getKeySV()));
                auto write = std::stoi(std::string(txnList[i].second[1]->getKeySV()));
                if (read != write) {
                    moneyList[read] = -100;
                    moneyList[write] = 100;
                }
            } else {
                aborted++;
            }
        }

        int totalSum = 0;
        for (int money: moneyList) {
            totalSum += money;
        }
        ASSERT_TRUE(totalSum == 0) << totalSum;
        LOG(INFO) << "Txn aborted: " << aborted;
    }

};

TEST_F(ReserveTableTest, TestSerial) {
    for(int i=0; i<100; i++) {
        testCase(nullptr);
    }
}

TEST_F(ReserveTableTest, TestParallel) {
    util::thread_pool_light tp(10);
    for(int i=0; i<100; i++) {
        testCase(&tp);
    }
}