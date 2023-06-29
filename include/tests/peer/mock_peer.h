//
// Created by user on 23-6-28.
//

#pragma once

#include "peer/core/user_rpc_controller.h"
#include "peer/storage/mr_block_storage.h"
#include "common/bccsp.h"
#include "common/yaml_key_storage.h"
#include "common/zeromq.h"
#include "common/zmq_port_util.h"
#include "proto/block.h"
#include "tests/mock_property_generator.h"

namespace tests::peer {
    class Peer {
    public:
        explicit Peer(const util::Properties &p) {
            auto node = p.getCustomPropertiesOrPanic("bccsp");
            bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
            CHECK(bccsp) << "Can not init bccsp";
            // create storage
            _blockStorage = std::make_shared<::peer::MRBlockStorage>(p.getNodeProperties().getGroupCount());
            auto portConfig = util::ZMQPortUtil::InitLocalPortsConfig(p);
            server = p.getNodeProperties().getLocalNodeInfo();
            auto rpcPort = portConfig->getLocalServicePorts(util::PortType::BFT_RPC)[server->nodeId];
            CHECK(::peer::core::UserRPCController::NewRPCController(_blockStorage, rpcPort));

            _subscriber = util::ZMQInstance::NewServer<zmq::socket_type::sub>(
                    portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[server->nodeId]);
            _collectorThread = std::make_unique<std::thread>(&Peer::collectorFunction, this);
            CHECK(::peer::core::UserRPCController::StartRPCService(rpcPort));
        }

        ~Peer() {
            _tearDownSignal = true;
            _collectorThread->join();
        }

    protected:
        void collectorFunction() {
            pthread_setname_np(pthread_self(), "mock_collector");
            int nextBlockId = 0;
            while(!_tearDownSignal.load(std::memory_order_relaxed)) {
                auto block = std::make_unique<::proto::Block>();

                do {
                    auto ret = _subscriber->receive();
                    if (ret == std::nullopt) {
                        return;  // socket dead
                    }
                    auto envelop = std::make_unique<proto::Envelop>();
                    envelop->setSerializedMessage(ret->to_string());
                    if (!envelop->deserializeFromString()) {
                        LOG(WARNING) << "Deserialize user request failed.";
                        continue;
                    }
                    block->body.userRequests.push_back(std::move(envelop));
                    auto reqSize = block->body.userRequests.size();
                    block->executeResult.transactionFilter.push_back(static_cast<std::byte>(reqSize%2));
                } while(block->body.userRequests.size() < 100);
                // validate the request
                auto& request = block->body.userRequests[0];
                auto key = bccsp->GetKey(request->getSignature().ski);
                auto ret = key->Verify(request->getSignature().digest,
                                       request->getPayload().data(),
                                       request->getPayload().size());
                CHECK(ret);
                block->header.number = nextBlockId++;
                _blockStorage->insertBlockAndNotify(server->groupId, std::move(block));
            }
        }

    private:
        std::atomic<bool> _tearDownSignal = false;
        std::shared_ptr<util::NodeConfig> server;
        std::unique_ptr<util::BCCSP> bccsp;
        std::shared_ptr<::peer::MRBlockStorage> _blockStorage;
        std::shared_ptr<util::ZMQInstance> _subscriber;
        std::unique_ptr<std::thread> _collectorThread;
    };
}