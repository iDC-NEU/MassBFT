//
// Created by user on 23-5-16.
//

#include "peer/core/module_coordinator.h"
#include "peer/core/module_factory.h"
#include "peer/core/single_pbft_controller.h"
#include "peer/consensus/block_order/global_ordering.h"
#include "peer/concurrency_control/deterministic/coordinator_impl.h"

namespace peer::core {

    ModuleCoordinator::~ModuleCoordinator() {
        _running = false;
        if (_subscriberThread) {
            _subscriberThread->join();
        }
    }

    std::unique_ptr<ModuleCoordinator> ModuleCoordinator::NewModuleCoordinator(const std::shared_ptr<util::Properties>& properties) {
        auto runningPath = properties->getRunningPath();
        std::filesystem::current_path(runningPath);
        auto mc = std::unique_ptr<ModuleCoordinator>(new ModuleCoordinator);
        auto nodeProperties = properties->getNodeProperties();
        mc->_localNode = nodeProperties.getLocalNodeInfo();
        if (mc->_localNode == nullptr) {
            return nullptr; // local config error
        }
        // 1.01 init concurrency control
        auto dbPath = std::filesystem::current_path().append("data").append(mc->_localNode->ski + "_db");
        if (!exists(dbPath) && !create_directories(dbPath)) {
            return nullptr; // create directory failed
        }
        mc->_db = peer::db::DBConnection::NewConnection(dbPath);
        if (mc->_db == nullptr) {
            return nullptr;
        }
        mc->_cc = peer::cc::CoordinatorImpl::NewCoordinator(mc->_db, properties->getAriaWorkerCount());
        if (mc->_cc == nullptr) {
            return nullptr;
        }
        // 1.02 init factory
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
            return ptr->_serialExecutor.addTask([=] {
                if (!ptr->onConsensusBlockOrder(chainId, blockNumber)) {
                    LOG(ERROR) << "Can not execute block.";
                }
            });
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
        auto totalGroup = nodeProperties.getGroupCount();
        // init user rpc controller (for pulling block)
        mc->_userRPCNotifier = mc->_moduleFactory->initUserRPCController();
        if (mc->_userRPCNotifier == nullptr) {
            return nullptr;
        }
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
        auto realBlock = _contentStorage->waitForBlock(regionId, blockId, 0);
        CHECK(realBlock != nullptr && (int)realBlock->header.number == blockId);
        // if success, txReadWriteSet and transactionFilter are the return values
        if (!_cc->processValidatedRequests(realBlock->body.userRequests,
                                           realBlock->executeResult.txReadWriteSet,
                                           realBlock->executeResult.transactionFilter)) {
            return false;
        }
        // NOTE: do not invoke realBlock->setSerializedMessage, not thread safe!
        if (_localNode->nodeId == 0) {
            DLOG(INFO) << "Leader of local group " << _localNode->groupId << " commit a block, chainId: " << regionId  << ", blockId: " << blockId;
        }
        // notify user by rpc
        _userRPCNotifier->insertBlockAndNotify(regionId, std::move(realBlock));
        return true;
    }

    void ModuleCoordinator::contentLeaderReceiverLoop() {
        pthread_setname_np(pthread_self(), "exec_receiver");
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

    bool ModuleCoordinator::startInstance() {
        if (!_moduleFactory->startReplicatorSender()) {
            LOG(ERROR) << "ReplicatorSender client start failed!";
            return false;
        }
        _localContentBFT->startInstance();
        return true;
    }

    bool ModuleCoordinator::initChaincodeData(const std::string& ccName) {
        auto orm = ::peer::chaincode::ORM::NewORMFromDBInterface(_db.get());
        auto cc = ::peer::chaincode::NewChaincodeByName(ccName, std::move(orm));
        if (cc->InitDatabase() != 0) {
            return false;
        }
        auto [reads, writes] = cc->reset();
        return _db->syncWriteBatch([&](auto* batch) ->bool {
            for (const auto& it: *writes) {
                // Never go wrong
                batch->Put({it->getKeySV().data(), it->getKeySV().size()}, {it->getValueSV().data(), it->getValueSV().size()});
            }
            return true;
        });
    }
}