//
// Created by user on 23-3-28.
//

#pragma once

#include "peer/consensus/pbft/local_consensus.h"
#include "peer/storage/mr_block_storage.h"
#include "common/pbft/pbft_rpc_service.h"
#include "common/zmq_port_util.h"

namespace peer::consensus::v2 {
    class LocalConsensusController {
    public:
        static std::unique_ptr<LocalConsensusController> NewLocalConsensusController(
                // All nodes from the same region
                // validateOnReceive: validate user request on receiving from user
                // turn off to optimistic trust user (will validate during consensus)
                const std::vector<std::shared_ptr<util::NodeConfig>>& localRegionNodes,
                int localId,
                const std::unique_ptr<util::ZMQPortUtil>& localPortConfig,
                const std::shared_ptr<util::BCCSP>& bccsp,
                std::shared_ptr<util::thread_pool_light> threadPoolForBCCSP,
                std::shared_ptr<peer::MRBlockStorage> storage,
                int timeoutMs,
                int maxBatchSize) {
            // check if localRegionNodes is in order
            for (int i=0; i<(int)localRegionNodes.size(); i++) {
                if (localRegionNodes[i]->nodeId != i) {
                    LOG(ERROR) << "localRegionNodes list must in order!";
                    return nullptr;
                }
            }
            // generate the corresponding zmq configs
            auto [payloadZMQConfigs, ret1] = util::ZMQPortUtil::WrapPortWithConfig(
                    localRegionNodes,
                    localPortConfig->getLocalServicePorts(util::PortType::BFT_PAYLOAD));
            if (!ret1) {
                LOG(ERROR) << "generate BFTPayloadSeparationPorts zmq config failed!";
                return nullptr;
            }
            auto [collectorZMQConfigs, ret2] = util::ZMQPortUtil::WrapPortWithConfig(
                    localRegionNodes,
                    localPortConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR));
            if (!ret2) {
                LOG(ERROR) << "generate RequestCollectorPorts zmq config failed!";
                return nullptr;
            }
            // Init ContentReplicator
            LocalConsensus::Config config{};
            config.localId = localId;
            config.maxBatchSize = maxBatchSize;
            config.timeoutMs = timeoutMs;
            config.userRequestPort = localPortConfig->getLocalServicePorts(util::PortType::USER_REQ_COLLECTOR)[localId];
            config.targetNodes = payloadZMQConfigs;

            auto sm = std::make_unique<LocalConsensus>(std::move(config));
            sm->setBCCSPWithThreadPool(bccsp, std::move(threadPoolForBCCSP));

            std::unique_ptr<LocalConsensusController> controller(new LocalConsensusController(
                    std::move(sm),
                    std::move(storage),
                    std::move(collectorZMQConfigs)));
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
            return controller;
        }

        LocalConsensusController(const LocalConsensusController&) = delete;

        LocalConsensusController(LocalConsensusController&&) = delete;

        ~LocalConsensusController() {
            if (rpcServerPort != -1) {
                _replicator->sendStopSignal();
                util::DefaultRpcServer::Stop(rpcServerPort);
            }
        }

        // if other service shared the same server, call this method maybe unnecessary
        [[nodiscard]] bool startRPCService() const {
            if (util::DefaultRpcServer::Start(rpcServerPort) != 0) {
                LOG(ERROR) << "Fail to start DefaultRpcServer at port: " << rpcServerPort;
                return false;
            }
            return true;
        }

    protected:
        LocalConsensusController(std::unique_ptr<LocalConsensus> replicator,
                            std::shared_ptr<peer::MRBlockStorage> storage,
                            // Cn be generated through ZMQPortUtil::WrapPortWithConfig
                            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> userCollectorPortAddr)
                : _userCollectorAddr(std::move(userCollectorPortAddr)) {
            _replicator = std::move(replicator);
            _storage = std::move(storage);
            // Wire the connections
            _replicator->setDeliverCallback([this](std::shared_ptr<::proto::Block> block, const ::util::NodeConfigPtr& localNode) {
                _storage->insertBlockAndNotify(localNode->groupId, std::move(block));
                return true;
            });
        }

    private:
        const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> _userCollectorAddr;
        int rpcServerPort = -1;
        std::shared_ptr<LocalConsensus> _replicator;
        std::shared_ptr<peer::MRBlockStorage> _storage;
    };
}