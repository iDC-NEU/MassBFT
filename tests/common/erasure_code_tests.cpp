//
// Created by peng on 11/21/22.
//

#include "common/erasure_code.h"

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

    static void encodeDecodeTest(util::ErasureCode& ec) {
        std::string dataEncode;
        for(int i=0; i<1000; i++) {
            dataEncode += std::to_string(i*3);
        }
        LOG(INFO) << "----construct and restore----";
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
        EXPECT_TRUE(dataDecode.substr(0, dataEncode.size()) == dataEncode);

        LOG(INFO) << "----Lost random 40 pieces----";
        std::vector<std::string_view> svListPartialView;
        for(int i=0; i<60; i++) {
            if (i%3+1 != 0) {
                svListPartialView.emplace_back("");
            } else {
                svListPartialView.push_back(svList[i]);
            }
        }
        decodeResult = ec.decode(svList);
        dataDecode = decodeResult->getData().value_or("");
        EXPECT_TRUE(dataDecode.substr(0, dataEncode.size()) == dataEncode);
    }

    static void encodeSizeTest(util::ErasureCode& ec, int m, int n) {
        std::string dataEncode;
        for(int i=0; i<200000; i++) {
            dataEncode += std::to_string(i*3);
        }
        LOG(INFO) << "----Encode / decode size test----";
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
        for(int i=0; i<(int)svList.size(); i++) {
            if (i<n) {
                svListPartialView.push_back(svList[i]);
            } else {
                svListPartialView.emplace_back("");
            }
        }
        timer.start();
        auto decodeResult = ec.decode(svList);
        auto spanDecode = timer.end();
        LOG(INFO) << "Lost m-n pieces, decode time: " << spanDecode;
        auto dataDecode = decodeResult->getData().value_or("");
        EXPECT_TRUE(dataDecode.substr(0, dataEncode.size()) == dataEncode);
        LOG(INFO) << "Percentage:" << double(m)/n << " vs " << double(erasureDataSize) / double(dataEncode.size());
    }

    static void encodePerformanceTest(util::ErasureCode& ec, int m, int n) {
        std::string dataEncode;
        for(int i=0; i<200000; i++) {
            dataEncode += std::to_string(i*3);
        }
        LOG(INFO) << "----Encode performance test----";
        LOG(INFO) << "original data size: " << dataEncode.size();
        util::Timer timer;
        for(int i=0; i<100; i++) {
            EXPECT_TRUE(ec.encode(dataEncode) != nullptr);
        }
        auto spanEncode = timer.end();
        LOG(INFO) << "Encode time: " << spanEncode;
        auto encodeResult = ec.encode(dataEncode);
        auto svList = encodeResult->getAll();
        EXPECT_TRUE((int)svList->size() == m);
        timer.start();
        for(int i=0; i<100; i++) {
            EXPECT_TRUE(ec.decode(svList.value()) != nullptr);
        }
        spanEncode = timer.end();
        LOG(INFO) << "Decode time: " << spanEncode;
    }

protected:
    moodycamel::LightweightSemaphore sema_;
    std::vector<std::thread> t_list_;
};

TEST_F(ESTest, ReconstructData) {
    util::GoErasureCode ec1(20, 40);
    LOG(INFO) << "GoErasureCode: ";
    encodeDecodeTest(ec1);
    util::LibErasureCode ec2(20, 40);
    LOG(INFO) << "LibErasureCode: ";
    encodeDecodeTest(ec2);
}

TEST_F(ESTest, SizeTest) {
    int m=900, n=300;
    util::GoErasureCode ec1(n, m-n);
    LOG(INFO) << "GoErasureCode: ";
    encodeSizeTest(ec1, m, n);
    util::LibErasureCode ec2(n, m-n);
    LOG(INFO) << "LibErasureCode: ";
    encodeSizeTest(ec2, m, n);
}

TEST_F(ESTest, EncodePerformance) {
    int m=900, n=300;
    util::GoErasureCode ec1(n, m-n);
    LOG(INFO) << "GoErasureCode: ";
    encodePerformanceTest(ec1, m, n);
    util::LibErasureCode ec2(n, m-n);
    LOG(INFO) << "LibErasureCode: ";
    encodePerformanceTest(ec2, m, n);
}

TEST_F(ESTest, MultiThreadReconstructData) {
    start(100);
    sleep(1);
    sema_.signal((int)t_list_.size());
    sleep(1);
}