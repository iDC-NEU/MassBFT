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
#include "peer/db/rocksdb_connection.h"
#include "peer/chaincode/orm.h"
#include "peer/chaincode/chaincode.h"

namespace tests::peer {
    class Peer {
    public:
        explicit Peer(const util::Properties &p, bool skipValidate = false) {
            auto node = p.getCustomPropertiesOrPanic("bccsp");
            bccsp = std::make_unique<util::BCCSP>(std::make_unique<util::YAMLKeyStorage>(node));
            CHECK(bccsp) << "Can not init bccsp";
            // create storage
            _blockStorage = std::make_shared<::peer::MRBlockStorage>(p.getNodeProperties().getGroupCount());
            auto portConfig = util::ZMQPortUtil::InitLocalPortsConfig(p);
            server = p.getNodeProperties().getLocalNodeInfo();
            rpcPort = portConfig->getLocalServicePorts(util::PortType::BFT_RPC)[server->nodeId];
            CHECK(::peer::core::UserRPCController::NewRPCController(_blockStorage, rpcPort));

            _subscriber = util::ZMQInstance::NewServer<zmq::socket_type::sub>(
                    portConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[server->nodeId]);
            _collectorThread = std::make_unique<std::thread>(&Peer::collectorFunction, this);
            CHECK(::peer::core::UserRPCController::StartRPCService(rpcPort));
            _blockSize = p.getBlockMaxBatchSize();
            _skipValidate = skipValidate;
            _dbc = ::peer::db::RocksdbConnection::NewConnection("YCSBChaincodeTestDB");
            CHECK(_dbc != nullptr) << "failed to init db!";
            _orm = ::peer::chaincode::ORM::NewORMFromLeveldb(_dbc.get());
            _chainCode = ::peer::chaincode::NewChaincodeByName("ycsb", std::move(_orm));
            _chainCode->InvokeChaincode("init", std::vector<std::string_view>{"10000"});
        }

        ~Peer() {
            if (_subscriber) {
                _tearDownSignal = true;
                _subscriber->shutdown();
            }
            if (_collectorThread) {
                _collectorThread->join();
            }
            if (rpcPort != 0) {
                ::peer::core::UserRPCController::StopRPCService(rpcPort);
            }
        }

        std::unordered_map<std::string_view, int> getOpCount() {
            return _opCount;
        }

    protected:
        void collectorFunction() {
            pthread_setname_np(pthread_self(), "mock_collector");
            int nextBlockId = 0;
            while(!_tearDownSignal.load(std::memory_order_relaxed)) {
                auto block = std::make_unique<::proto::Block>();
                block->executeResult.transactionFilter.reserve(_blockSize);
                block->body.userRequests.reserve(_blockSize);

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

                    // deserialize the request payload
                    auto& request = block->body.userRequests.back();
                    proto::UserRequest user;
                    zpp::bits::in in(request->getPayload());
                    if (failure(in((user)))) {
                        return;
                    }
                    // record the count of every operation
                    _opCount[user.getFuncNameSV()] += 1;

                    std::vector<std::string_view> args{user.getArgs()};
                    auto executeRet = _chainCode->InvokeChaincode(user.getFuncNameSV(), args);
                    if(executeRet == 0) {
                        block->executeResult.transactionFilter.push_back(static_cast<std::byte>(1));
                    } else {
                        block->executeResult.transactionFilter.push_back(static_cast<std::byte>(0));
                    }
                } while((int)block->body.userRequests.size() < _blockSize);
                if (!_skipValidate) {
                    // validate the request
                    auto& request = block->body.userRequests[0];
                    auto key = bccsp->GetKey(request->getSignature().ski);
                    auto ret = key->Verify(request->getSignature().digest,
                                           request->getPayload().data(),
                                           request->getPayload().size());
                    CHECK(ret);
                }
                block->header.number = nextBlockId++;
                _blockStorage->insertBlockAndNotify(server->groupId, std::move(block));
            }
        }

    private:
        std::atomic<bool> _tearDownSignal = false;
        int rpcPort = 0;
        std::shared_ptr<util::NodeConfig> server;
        std::unique_ptr<util::BCCSP> bccsp;
        int _blockSize = 0;
        bool _skipValidate;
        std::shared_ptr<::peer::MRBlockStorage> _blockStorage;
        std::shared_ptr<util::ZMQInstance> _subscriber;
        std::unique_ptr<std::thread> _collectorThread;
        std::unique_ptr<::peer::db::RocksdbConnection> _dbc;
        std::unique_ptr<::peer::chaincode::ORM> _orm;
        std::unordered_map<std::string_view, int> _opCount;
        std::unique_ptr<::peer::chaincode::Chaincode> _chainCode;
    };
}