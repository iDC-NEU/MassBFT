//
// Created by peng on 11/5/22.
//

#include "common/thread_local_store.h"
#include "common/timer.h"

#include "gtest/gtest.h"
#include "lightweightsemaphore.h"
#include <vector>
#include <thread>

class TLSTest : public ::testing::Test {
protected:
    void SetUp() override {
    };

    void TearDown() override {
        for(auto& t: t_list_) {
            if(t.joinable()) {
                t.join();
            }
        }
        t_list_.clear();
    };

    void start(int thread_cnt) {
        auto createMulti = [this](auto id) {
            *util::ThreadLocalStore<int>::Get() = id;
            sema_.wait();
            EXPECT_EQ(id, *util::ThreadLocalStore<int>::Get());
        };
        for (auto i=0; i<thread_cnt; i++) {
            t_list_.template emplace_back(createMulti, i);
        }
        sema_.signal(thread_cnt);
    }

protected:
    moodycamel::LightweightSemaphore sema_;
    std::vector<std::thread> t_list_;
};

TEST_F(TLSTest, MultiGetStore) {
    start(100);
    util::Timer::sleep_sec(1);
    sema_.signal((int)t_list_.size());
    util::Timer::sleep_sec(1);
}
