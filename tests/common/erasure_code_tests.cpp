//
// Created by peng on 11/21/22.
//

#include "common/erasure_code.h"
#include "libgrc.h"

#include "gtest/gtest.h"
#include "concurrentqueue.h"
#include "lightweightsemaphore.h"
#include "common/timer.h"
#include <vector>
#include <thread>

class ESTest : public ::testing::Test {
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

TEST_F(ESTest, ReconstructData) {
    util::ErasureCode ec(20, 40);
    std::string dataEncode;
    for(int i=0; i<1000; i++) {
        dataEncode += std::to_string(i*3);
    }
    LOG(INFO) << "original data size: " << dataEncode.size();
    auto encodeResult = ec.encode(dataEncode);
    std::vector<std::string_view> svList;
    size_t erasureDataSize = 0;
    for(int i=0; ; i++) {
        auto fragment = encodeResult->get(i);
        if(!fragment) {
            EXPECT_EQ(i, 60) << "fragment count not equal 60";
            break;
        }
        EXPECT_TRUE(!fragment->empty()) << "fragment is empty";
        svList.push_back(fragment.value());
        erasureDataSize += fragment->size();
    }
    LOG(INFO) << "Erasure data size(60 pieces): " << erasureDataSize;
    auto decodeResult = ec.decode(svList);
    auto dataDecode = decodeResult->getData().value_or("");
    EXPECT_TRUE(dataDecode == dataEncode);
    LOG(INFO) << "Lost random 40 pieces";
    std::vector<std::string_view> svListPartialView;
    for(int i=0; i<20; i++) {
        svListPartialView.push_back(svList[1*3]);
    }
    EXPECT_TRUE(svListPartialView.size() == 20);
    decodeResult = ec.decode(svList);
    dataDecode = decodeResult->getData().value_or("");
    EXPECT_TRUE(dataDecode == dataEncode);
}

TEST_F(ESTest, SizeTest) {
    int m=900, n=300;
    util::ErasureCode ec(n, m-n); // m=4, n=1
    std::string dataEncode;
    for(int i=0; i<200000; i++) {
        dataEncode += std::to_string(i*3);
    }
    LOG(INFO) << "original data size: " << dataEncode.size();
    util::Timer timer;
    auto encodeResult = ec.encode(dataEncode);
    auto spanEncode = timer.end();
    std::vector<std::string_view> svList;
    size_t erasureDataSize = 0;
    for(int i=0; ; i++) {
        auto fragment = encodeResult->get(i);
        if(!fragment) {
            break;
        }
        EXPECT_TRUE(!fragment->empty()) << "fragment is empty";
        svList.push_back(fragment.value());
        erasureDataSize += fragment->size();
    }
    LOG(INFO) << "Erasure data size: " << erasureDataSize << ", encode time: " << spanEncode;
    std::vector<std::string_view> svListPartialView;
    for(int i=0; i<n; i++) {
        svListPartialView.push_back(svList[i]);
    }
    timer.start();
    auto decodeResult = ec.decode(svList);
    auto spanDecode = timer.end();
    LOG(INFO) << "Lost random m-n pieces, decode time: " << spanDecode;
    auto dataDecode = decodeResult->getData().value_or("");
    EXPECT_TRUE(dataDecode == dataEncode);
    LOG(INFO) << "Percentage:" << double(m)/n << " vs " << double(erasureDataSize) / double(dataEncode.size());
}

TEST_F(ESTest, EncodePerformance) {
    int m=900, n=300;
    util::ErasureCode ec(n, m-n); // m=4, n=1
    std::string dataEncode;
    for(int i=0; i<200000; i++) {
        dataEncode += std::to_string(i*3);
    }
    LOG(INFO) << "original data size: " << dataEncode.size();
    util::Timer timer;
    for(int i=0; i<1000; i++) {
        ec.encode(dataEncode);
    }
    auto spanEncode = timer.end();
    LOG(INFO) << "Encode time: " << spanEncode;
}

TEST_F(ESTest, MultiThreadReconstructData) {
    auto id = instanceCreate(10,10);
    LOG(INFO) <<id;
    start(100);
    sleep(1);
    sema_.signal((int)t_list_.size());
    sleep(1);
}