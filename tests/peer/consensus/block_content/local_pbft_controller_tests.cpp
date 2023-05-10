//
// Created by user on 23-3-29.
//

#include "peer/consensus/block_content/local_pbft_controller.h"

#include "tests/proto_block_utils.h"
#include "gtest/gtest.h"
#include "glog/logging.h"

class LocalPBFTControllerTest : public ::testing::Test {
public:
    LocalPBFTControllerTest() {
        util::OpenSSLED25519::initCrypto();

        auto nodesZMQConfigs = tests::ProtoBlockUtils::GenerateNodesConfig(0, 4, 1000);
        for (const auto& it: nodesZMQConfigs) {
            nodesConfig.emplace_back(it->nodeConfig);
        }
        std::vector<int> regionServerCount {4, 4, 4};
        int offset = 51200;
        for (int i=0; i<4; i++) {
            portsConfig.emplace_back(new peer::v2::SingleServerZMQPortUtil(regionServerCount, 0, i, offset));
        }

        auto ms = std::make_unique<util::DefaultKeyStorage>();
        bccsp = std::make_shared<util::BCCSP>(std::move(ms));
        threadPool = std::make_unique<util::thread_pool_light>(4);   // 4 threads
        for (const auto& it: nodesZMQConfigs) {
            auto key = bccsp->generateED25519Key(it->nodeConfig->ski, false);
            CHECK(key != nullptr);
        }
        for (int i=0; i<4; i++) {
            auto storage = std::make_shared<peer::MRBlockStorage>(3);   // 3 regions
            storageList.push_back(storage);
            auto controller = peer::consensus::LocalPBFTController<false>::NewPBFTController(nodesConfig, i, portsConfig[i], bccsp, threadPool, storage, {100, 200});
            CHECK(controller != nullptr) << "init controller error!";
            controllerList.push_back(std::move(controller));
        }
    }

    // must NOT return nullptr
    auto createSignedEnvelop(int nodeId) {
        auto& ski = nodesConfig[nodeId]->ski;
        std::unique_ptr<proto::Envelop> envelop(new proto::Envelop());
        proto::SignatureString sig = {
                ski,
                std::make_shared<std::string>()};
        auto key = bccsp->GetKey(ski);
        CHECK(key->Private());
        std::string payload("payload for an envelop" + std::to_string(rand()));
        auto ret = key->Sign(payload.data(), payload.size());
        CHECK(ret != std::nullopt);
        sig.digest = *ret;
        envelop->setPayload(std::move(payload));
        envelop->setSignature(std::move(sig));
        return envelop;
    }

protected:
    void SetUp() override {
    };

    void TearDown() override {
    };

protected:
    std::vector<util::NodeConfigPtr> nodesConfig;
    std::vector<std::unique_ptr<peer::v2::ZMQPortUtil>> portsConfig;
    std::shared_ptr<util::BCCSP> bccsp;
    std::shared_ptr<util::thread_pool_light> threadPool;
    std::vector<std::shared_ptr<peer::MRBlockStorage>> storageList;
    std::vector<std::unique_ptr<peer::consensus::LocalPBFTController<false>>> controllerList;
};

TEST_F(LocalPBFTControllerTest, TestCreate) {
    auto client0 = util::ZMQInstance::NewClient<zmq::socket_type::push>("127.0.0.1", portsConfig[0]->getRequestCollectorPorts()[0]);
    auto client1 = util::ZMQInstance::NewClient<zmq::socket_type::push>("127.0.0.1", portsConfig[0]->getRequestCollectorPorts()[1]);
    auto client2 = util::ZMQInstance::NewClient<zmq::socket_type::push>("127.0.0.1", portsConfig[0]->getRequestCollectorPorts()[2]);
    util::Timer::sleep_sec(30);  // run TEST_F(ControllerTest, StartDemoInstance) for fast distribution
    for (int i=0; i<200 * 100; i++) {
        auto envelop = createSignedEnvelop(i%4);
        std::string buf;
        CHECK(envelop->serializeToString(&buf));
        client0->send(buf);
        client1->send(buf);
        client2->send(buf);
    }
    sleep(3600);
}