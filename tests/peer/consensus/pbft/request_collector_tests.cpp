//
// Created by user on 23-3-28.
//

#include "peer/consensus/pbft/request_collector.h"

#include "tests/proto_block_utils.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

class RequestCollectorTest : public ::testing::Test {
public:
    RequestCollectorTest() {
        config.port = 51200;
        config.timeoutMs = 100;
        config.maxBatchSize = 10;
    }
protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

protected:
    peer::consensus::RequestCollector::Config config{};
};

TEST_F(RequestCollectorTest, TestNormalCase) {
    peer::consensus::RequestCollector collector(config);

    int totalRequests = 0;
    int totalBatches = 0;
    collector.setBatchCallback([&](const std::vector<std::unique_ptr<proto::Envelop>>& unorderedRequests) {
        LOG(INFO) << "Get a batch, size: " << unorderedRequests.size();
        totalRequests += (int)unorderedRequests.size();
        totalBatches++;
        return true;
    });
    collector.start();
    auto client = util::ZMQInstance::NewClient<zmq::socket_type::pub>("127.0.0.1", 51200);
    auto envelop = tests::ProtoBlockUtils::CreateMockEnvelop();
    std::string buf;
    CHECK(envelop->serializeToString(&buf));
    util::Timer::sleep_sec(0.5);
    for (int i=0; i<20; i++) {
        client->send(buf);
    }
    util::Timer::sleep_sec(0.5);
    ASSERT_TRUE(totalRequests == 20);
}
