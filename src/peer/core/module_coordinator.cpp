//
// Created by user on 23-5-16.
//

#include "peer/core/module_coordinator.h"
#include "peer/core/bootstrap.h"
#include "peer/core/single_pbft_controller.h"
#include "peer/consensus/block_order/global_ordering.h"

namespace peer::core {

    ModuleCoordinator::~ModuleCoordinator() {
        _running = false;
        if (_subscriberThread) {
            _subscriberThread->join();
        }
    }

    std::unique_ptr<ModuleCoordinator> ModuleCoordinator::NewModuleCoordinator(const std::shared_ptr<util::Properties>& properties) {
        auto mc = std::unique_ptr<ModuleCoordinator>(new ModuleCoordinator);
        mc->_moduleFactory = peer::core::ModuleFactory::NewModuleFactory(properties);
        if (mc->_moduleFactory == nullptr) {
            return nullptr;
        }
        // 1.1 first init content storage
        mc->_contentStorage = mc->_moduleFactory->getOrInitContentStorage();
        if (mc->_contentStorage == nullptr) {
            return nullptr;
        }
        // 1.2 init GlobalBlockOrdering
        auto orderCAB = std::make_shared<peer::consensus::v2::OrderACB>([ptr = mc.get()](int chainId, int blockNumber) {
            return ptr->onConsensusBlockOrder(chainId, blockNumber);
        });
        auto gbo = mc->_moduleFactory->newGlobalBlockOrdering(orderCAB);
        if (gbo == nullptr) {
            return nullptr;
        }
        mc->_gbo = std::move(gbo);
        // 1.3 replicator automatically get blocks from local storage
        auto replicator = mc->_moduleFactory->getOrInitReplicator();
        if (replicator == nullptr) {
            return nullptr;
        }
        // 1.4 init user request collector
        auto nodeProperties = properties->getNodeProperties();
        mc->_localNode = nodeProperties.getLocalNodeInfo();
        auto totalGroup = nodeProperties.getGroupCount();
        // the bftController id is 0
        auto bftController = mc->_moduleFactory->newReplicatorBFTController(0*totalGroup + mc->_localNode->groupId);
        if (bftController == nullptr) {
            return nullptr;
        }
        mc->_localContentBFT = std::move(bftController);
        // the result will be pushed into storage automatically

        // spin up the thread finally
        if (mc->_gbo->isLeader()) {
            // add a listener on mr block storage, only as leader
            mc->_subscriberId = mc->_contentStorage->newSubscriber();
            mc->_subscriberThread = std::make_unique<std::thread>(&ModuleCoordinator::contentLeaderReceiverLoop, mc.get());
        }
        return mc;
    }

    // called after generated final block order by GlobalBlockOrdering
    bool ModuleCoordinator::onConsensusBlockOrder(int regionId, int blockId) {
        // Todo: bind real element, handle the block to executor
        auto realBlock = _contentStorage->waitForBlock(regionId, blockId, 0);
        CHECK(realBlock != nullptr && (int)realBlock->header.number == blockId);
        if (_localNode->groupId == 0 && _localNode->nodeId == 0) {
            LOG(INFO) << "Finally receive a block (" << regionId << ", " << blockId << ", " << realBlock->body.userRequests.size() << ")" << ", threadId: " << std::this_thread::get_id();
        }
        return true;
    }

    void ModuleCoordinator::contentLeaderReceiverLoop() {
        CHECK(_gbo->isLeader()) << "node must be leader to invoke this function!";
        while(_running) {
            auto [regionId, block] = _contentStorage->subscriberWaitForBlock(_subscriberId, 1000);
            if (block == nullptr) {
                continue;   // retry after 1000 ms
            }
            // DLOG(INFO) << "Receive a block, regionId: " << regionId << ", block number: " << block->header.number << ", threadId: " << std::this_thread::get_id();
            auto blockId = (int)block->header.number;
            // push it into the global ordering module
            auto ret = _gbo->voteNewBlock(regionId, blockId);
            if (!ret) { // only leader can do this
                LOG(ERROR) << "Consensus block failed, regionId: " << regionId << ", block number: " << blockId;
            }
        }
    }

    void ModuleCoordinator::waitInstanceReady() const {
        // wait until raft is ready
        if (_gbo->isLeader() && !_gbo->waitUntilRaftReady()) {
            LOG(ERROR) << "Raft config contains error, can not wait until raft is ready!";
        }
        // wait until bft is ready
        _localContentBFT->waitUntilReady();
    }

    void ModuleCoordinator::startInstance() {
        if (!_moduleFactory->startReplicatorSender()) {
            CHECK(false) << "ReplicatorSender client start failed!";
        }
        _localContentBFT->startInstance();
    }
}