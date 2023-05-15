//
// Created by user on 23-3-28.
//

#pragma once

#include "peer/consensus/block_content/content_replicator.h"
#include "peer/consensus/block_content/request_collector.h"
#include "peer/storage/mr_block_storage.h"
#include "common/pbft/pbft_rpc_service.h"
#include "common/zmq_port_util.h"

namespace peer::consensus {
    class LocalPBFTController {
    public:
        static std::unique_ptr<LocalPBFTController> NewPBFTController(
                // All nodes from the same region
                // validateOnReceive: validate user request on receiving from user
                // turn off to optimistic trust user (will validate during consensus)
                const std::vector<std::shared_ptr<util::NodeConfig>>& localRegionNodes,
                int localId,
                const std::unique_ptr<util::ZMQPortUtil>& localPortConfig,
                const std::shared_ptr<util::BCCSP>& bccsp,
                std::shared_ptr<util::thread_pool_light> threadPoolForBCCSP,
                std::shared_ptr<peer::MRBlockStorage> storage,
                const RequestCollector::Config& batchConfig,
                bool validateOnReceive) {
            // check if localRegionNodes is in order
            for (int i=0; i<(int)localRegionNodes.size(); i++) {
                if (localRegionNodes[i]->nodeId != i) {
                    LOG(ERROR) << "localRegionNodes list must in order!";
                    return nullptr;
                }
            }
            // generate the corresponding zmq configs
            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> payloadZMQConfigs;
            if (!util::ZMQPortUtil::WrapPortWithConfig(localRegionNodes,
                                                           localPortConfig->getLocalServicePorts(util::PortType::BFT_PAYLOAD),
                                                           payloadZMQConfigs)) {
                LOG(ERROR) << "generate BFTPayloadSeparationPorts zmq config failed!";
                return nullptr;
            }
            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> collectorZMQConfigs;
            if (!util::ZMQPortUtil::WrapPortWithConfig(localRegionNodes,
                                                       localPortConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR),
                                                       collectorZMQConfigs)) {
                LOG(ERROR) << "generate RequestCollectorPorts zmq config failed!";
                return nullptr;
            }
            // Init ContentReplicator
            auto sm = std::make_unique<ContentReplicator>(payloadZMQConfigs, localId);
            sm->setBCCSPWithThreadPool(bccsp, std::move(threadPoolForBCCSP));
            auto rc = std::make_unique<RequestCollector>(batchConfig, localPortConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[localId]);
            std::unique_ptr<LocalPBFTController> controller(
                    new LocalPBFTController(std::move(sm),
                                            std::move(rc),
                                            std::move(storage),
                                            std::move(collectorZMQConfigs),
                                            validateOnReceive));
            // When the system is just started, all user requests received will be discarded
            // because the primary node is not determined. Thus, no need to call OnBecomeFollower(nullptr);
            if (!controller->_replicator->checkAndStart()) {
                LOG(ERROR) << "Replicator service start failed!";
                return nullptr;
            }
            auto service = new util::pbft::PBFTRPCService();
            if(!service->checkAndStart(localRegionNodes, bccsp, controller->_replicator)) {
                LOG(ERROR) << "Fail to start PBFTRPCService!";
                return nullptr;
            }
            auto rpcPort = localPortConfig->getLocalServicePorts(util::PortType::BFT_RPC).at(localId);
            controller->rpcServerPort = rpcPort;
            if (util::DefaultRpcServer::AddService(service, rpcPort) != 0) {
                LOG(ERROR) << "Fail to add globalControlService!";
                return nullptr;
            }
            if (util::DefaultRpcServer::Start(rpcPort) != 0) {
                LOG(ERROR) << "Fail to start DefaultRpcServer at port: " << rpcPort;
                return nullptr;
            }
            return controller;
        }

        LocalPBFTController(const LocalPBFTController&) = delete;

        LocalPBFTController(LocalPBFTController&&) = delete;

        ~LocalPBFTController() {
            if (rpcServerPort != -1) {
                _replicator->sendStopSignal();
                util::DefaultRpcServer::Stop(rpcServerPort);
            }
        }

    protected:
        LocalPBFTController(std::unique_ptr<ContentReplicator> replicator,
                            std::unique_ptr<RequestCollector> collector,
                            std::shared_ptr<peer::MRBlockStorage> storage,
                            // Cn be generated through ZMQPortUtil::WrapPortWithConfig
                            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> userCollectorPortAddr,
                            bool validateOnReceive)
                            : _userCollectorAddr(std::move(userCollectorPortAddr)), _validateOnReceive(validateOnReceive) {
            _replicator = std::move(replicator);
            _collector = std::move(collector);
            _storage = std::move(storage);
            // Wire the connections
            if (_validateOnReceive) {
                _collector->setValidateCallback([this](const auto& envelop) {
                    return _replicator->validateUserRequest(envelop);
                }, _replicator->getThreadPoolForBCCSP());
            }
            _replicator->setDeliverCallback([this](std::shared_ptr<::proto::Block> block, const ::util::NodeConfigPtr& localNode) {
                auto blockNumber = block->header.number;
                _storage->insertBlock(localNode->groupId, std::move(block));
                // wake up all consumer
                _storage->onReceivedNewBlock(localNode->groupId, blockNumber);
                _storage->onReceivedNewBlock();
                return true;
            });
            _replicator->setLeaderChangeCallback([this](auto&&, auto&& newLeaderNode, int) {
                if (newLeaderNode == nullptr) {
                    // current node is leader
                    OnBecomeLeader();
                    return;
                }
                // When a node becomes a slave node, it should forward all incoming transaction requests to the master node.
                // TODO: The current version may have verified these user requests by slave nodes before forwarding them,
                //  causing additional computation and latency overhead.
                //  In addition, the forwarding request pipeline itself also causes some additional overhead.
                OnBecomeFollower(std::forward<decltype(newLeaderNode)>(newLeaderNode));
            });
        }

    protected:
        // When the node becomes the leader, it starts to receive messages from users
        void OnBecomeLeader() {
            _collector->start();
            _collector->setBatchCallback([this](auto&& item) {
                return _replicator->pushUnorderedBlock(std::forward<decltype(item)>(item), _validateOnReceive);
            });
        }

        // When the node becomes a follower, it stops receiving messages and
        // forwards the received messages to the new leader
        void OnBecomeFollower(const ::util::NodeConfigPtr& newLeaderNode) {
            for (const auto& it: _userCollectorAddr) {
                if (it->nodeConfig->ski == newLeaderNode->ski) {
                    // found the target config, create a client and forward requests to it
                    _redirectClient = util::ZMQInstance::NewClient<zmq::socket_type::pub>(it->priAddr(), it->port);
                    _collector->setBatchCallback([&](const std::vector<std::unique_ptr<::proto::Envelop>>& items) {
                        for (const auto& item: items) {
                            if (item->haveSerializedMessage()) {    // has cached message
                                _redirectClient->send(*item->getSerializedMessage());
                            } else {    // generate cached message
                                std::string buf;
                                item->serializeToString(&buf);
                                _redirectClient->send(std::move(buf));
                            }
                        }
                        return true;  // redirect requests to leader
                    });
                    LOG(INFO) << "Local node begin to redirect serve user requests to: " << newLeaderNode->ski;
                    return; // found the node
                }
            }   // end for
            LOG(WARNING) << "Can not find node, ski: " << newLeaderNode->ski;
        }

    private:
        const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> _userCollectorAddr;
        const bool _validateOnReceive;
        int rpcServerPort = -1;
        std::unique_ptr<util::ZMQInstance> _redirectClient;
        std::shared_ptr<ContentReplicator> _replicator;
        std::unique_ptr<RequestCollector> _collector;
        std::shared_ptr<peer::MRBlockStorage> _storage;
    };
}