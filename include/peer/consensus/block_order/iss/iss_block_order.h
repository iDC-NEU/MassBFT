//
// Created by user on 23-9-14.
//

#pragma once

#include "peer/consensus/block_order/round_based/round_based_agreement.h"
#include "peer/consensus/block_order/round_based/round_based_order_manager.h"
#include "peer/consensus/block_order/block_order.h"
#include "peer/storage/mr_block_storage.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "common/lru.h"
#include "common/cv_wrapper.h"

namespace peer::consensus::iss {
    // each epoch multiplexing multiple segments into a final totally ordered log.
    // calculates 𝐿𝑒𝑎𝑑𝑒𝑟𝑠(𝑒), the set of nodes that will act as leaders in 𝑒, based on the used leader selection policy
    // Answer: we use all group leader to maximize performance
    // epoch and block number both start from 0
    class EpochManager {
    public:
        explicit EpochManager(int epochLength, int groupCount)
                : _epochLength(epochLength), _groupCount(groupCount) { }

        // call before apply() to raft
        void waitUntilCanPropose(int blockNumber) const {
            condition.wait([&]{
                // LOG(INFO) << "Wait for block " << blockNumber << ", " << _epochLength * (_currentEpoch + 1) << ", " << blockNumber;
                if (_epochLength * (_currentEpoch + 1) <= blockNumber) {
                    return false;   // can not propose
                }
                return true;    // condition met
            });
        }

        // call after onApply()
        void receivedBlock(int chainId, int blockId) {
            std::lock_guard guard(mutex);
            // LOG(INFO) << "Receive a block with " << chainId << ", " << blockId;
            if (!_bucket.contains(blockId)) {
                _bucket.insert(blockId, std::make_shared<std::unordered_set<int>>());
            }
            auto& value = _bucket.getRef(blockId);
            value->insert(chainId);

            // check if condition met
            auto startCheckFrom = _currentEpoch;
            while (checkEpoch(startCheckFrom)) {
                // Requiring a node to have committed all batches in epoch 𝑒 before proposing batches for 𝑒+1
                // prevents request duplication across epochs. When a node transitions from 𝑒 to 𝑒 + 1,
                // no requests are “in flight”—each request has either already been committed in 𝑒
                // or has not yet been proposed in 𝑒 + 1.
                startCheckFrom += 1;
                // LOG(INFO) << "Notify epoch " << startCheckFrom;
                condition.notify_all([&]{
                    _currentEpoch = startCheckFrom;
                });
            }
        }

    protected:
        bool checkEpoch(int epochNumber) {
            // from: _epochLength * _currentEpoch
            // to: _epochLength * (_currentEpoch + 1) - 1
            for (int i = _epochLength * epochNumber; i < _epochLength * (epochNumber + 1); i++) {
                if (!_bucket.contains(i)) {
                    return false; // not finished!
                }
                auto& res = _bucket.getRef(i);
                if ((int)res->size() < _groupCount) {
                    return false; // not finished!
                }
            }
            // LOG(INFO) << "Epoch " << epochNumber << " is finished!";
            return true;    // epoch is finished!
        }

    private:
        mutable util::CVWrapper condition;
        mutable std::mutex mutex;
        const int _epochLength;
        const int _groupCount;
        int _currentEpoch = 0;
        util::LRUCache<int, std::shared_ptr<std::unordered_set<int>>> _bucket;
    };

    // numBuckets = number of leaders
    class ISSCallback : public v2::RaftCallback {
    public:
        explicit ISSCallback(std::shared_ptr<::peer::MRBlockStorage> storage)
                :_storage(std::move(storage)) {
            if (_storage == nullptr) {
                LOG(WARNING) << "Storage is empty, validator may not wait until receiving the actual block.";
            }

            setOnErrorCallback([this](int subChainId) {
                return _orderManager->invalidateChain(subChainId);
            });

            setOnValidateCallback([this](const std::string& decision)->bool {
                if (_storage != nullptr) {
                    return this->waitUntilReceiveValidBlock(decision);
                }
                return true;
            });

            setOnBroadcastCallback([this](const std::string& decision)->bool {
                return applyRawBlockOrder(decision);
            });
        }

        void init(int groupCount, std::unique_ptr<v2::LocalDistributor> ld) override {
            v2::RaftCallback::init(groupCount, std::move(ld));
            auto om = std::make_unique<rb::RoundBasedOrderManager>();
            om->setSubChainCount(groupCount);
            om->setDeliverCallback([this](int chainId, int blockId) {
                if (!onExecuteBlock(chainId, blockId)) {
                    LOG(ERROR) << "Execute block failed, bid: " << blockId;
                }
            });
            _orderManager = std::move(om);
            // about 23.27 round per epoch
            auto em = std::make_unique<EpochManager>(23, groupCount);
            _epochManager = std::move(em);
        }

    protected:
        bool applyRawBlockOrder(const std::string& decision) {
            int chainId, blockId;
            zpp::bits::in in(decision);
            if(failure(in(chainId, blockId))) {
                return false;
            }
            _epochManager->receivedBlock(chainId, blockId);
            return _orderManager->pushDecision(chainId, blockId);
        }

        [[nodiscard]] bool waitUntilReceiveValidBlock(const std::string& decision) const {
            int chainId, blockId;
            zpp::bits::in in(decision);
            if(failure(in(chainId, blockId))) {
                return false;
            }
            for (int i=0; _storage->waitForBlock(chainId, blockId, 40) == nullptr; i++) {
                if (i == 5) {
                    LOG(WARNING) << "Cannot get block after 5 tries";
                    return false;
                }
            }
            return true;
        }

    public: // ugly code
        void waitUntilCanPropose(int blockNumber) {
            return _epochManager->waitUntilCanPropose(blockNumber);
        }

    private:
        std::unique_ptr<rb::RoundBasedOrderManager> _orderManager;
        std::shared_ptr<::peer::MRBlockStorage> _storage;
        std::unique_ptr<EpochManager> _epochManager;
    };

    class BlockOrder : public BlockOrderInterface {
    public:
        static std::unique_ptr<v2::RaftCallback> NewRaftCallback(std::shared_ptr<::peer::MRBlockStorage> storage,
                                                                 const std::shared_ptr<util::BCCSP>&,
                                                                 const std::shared_ptr<util::thread_pool_light>&) {
            return std::make_unique<ISSCallback>(std::move(storage));
        }

        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &localReceivers,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &multiRaftParticipant,
                const std::vector<int> &multiRaftLeaderPos,  // the leader indexes in multiRaftParticipant
                const util::NodeConfigPtr &me,
                std::shared_ptr<v2::RaftCallback> callback) {
            auto bo = std::make_unique<BlockOrder>();
            {   // init LocalDistributor
                // find local nodes position
                int mePos = -1;
                for (int i = 0; i < (int) localReceivers.size(); i++) {
                    if (*localReceivers[i]->nodeConfig == *me) {
                        mePos = i;
                        break;
                    }
                }
                if (mePos == -1) {  // not exist
                    LOG(ERROR) << "Can not find current node in localReceivers!";
                    return nullptr;
                }
                auto ld = v2::LocalDistributor::NewLocalDistributor(localReceivers, mePos);
                if (ld == nullptr) {
                    LOG(ERROR) << "create LocalDistributor failed!";
                    return nullptr;
                }
                int maxGroupId = 0;
                for (auto &it: multiRaftParticipant) {
                    maxGroupId = std::max(maxGroupId, it->nodeConfig->groupId);
                }
                callback->init(maxGroupId + 1, std::move(ld));
                bo->agreementCallback = std::move(callback);
            }
            {   // init AsyncAgreement
                //---- get group count and local instance ID
                std::shared_ptr<util::ZMQInstanceConfig> localConfig = nullptr;
                for (int i=0; i<(int)multiRaftParticipant.size(); i++) {
                    if (*me == *multiRaftParticipant[i]->nodeConfig) {
                        localConfig = multiRaftParticipant[i];
                        for (auto j: multiRaftLeaderPos) {
                            if (i == j) {
                                bo->isRaftLeader = true;
                                break;
                            }
                        }
                        break;
                    }
                }
                if (localConfig != nullptr) {   // init aa
                    auto aa = rb::RoundBasedAgreement::NewRoundBasedAgreement(localConfig, bo->agreementCallback);
                    if (aa == nullptr) {
                        LOG(ERROR) << "create AsyncAgreement failed!";
                        return nullptr;
                    }
                    // start raft group
                    for (const auto& it: multiRaftLeaderPos) {
                        if (!aa->startCluster(multiRaftParticipant, it)) {
                            LOG(ERROR) << "start up multi raft failed!";
                            return nullptr;
                        }
                    }
                    bo->localGroupId = localConfig->nodeConfig->groupId;
                    bo->raftAgreement = std::move(aa);
                }
            }
            return bo;
        }

        // only raft leader can invoke this function
        bool voteNewBlock(int chainId, int blockId) override {
            if (!isRaftLeader) {    // local node must be raft leader
                return false;
            }
            if (localGroupId != chainId) {
                return true;    // skip block from other groups
            }
            // Fix the bug caused by concurrent queue
            // if we have block 0 and block 3, then we must also have block 1 and block 2, just consensus them (is safe)
            // note: this is only true when sub chain id == localGroupId
            for (int i=lastBlockId+1; i<=blockId; i++) {
                if (!voteNewBlockInner(i)) {    // skip blocks that are already proposed
                    return false;
                }
            }
            lastBlockId = std::max(lastBlockId, blockId);
            return true;
        }

    protected:
        bool voteNewBlockInner(int blockId) {
            // LOG(INFO) << "Group " << localGroupId << " leader vote for new epoch " << blockId;
            // ISS advances from epoch 𝑒 to epoch 𝑒 + 1 when the log contains an entry for each sequence number in 𝑆𝑛(𝑒)
            // ISS: wait until current epoch is finished!
            auto* ptr = dynamic_cast<ISSCallback*>(agreementCallback.get());
            CHECK(ptr != nullptr) << "cast failed!";
            ptr->waitUntilCanPropose(blockId);
            return raftAgreement->onReplicatingNewBlock(localGroupId, blockId);
        }

    public:
        [[nodiscard]] bool isLeader() const override { return isRaftLeader; }

        // wait until the node become the leader of the raft group
        [[nodiscard]] bool waitUntilRaftReady() const override {
            if (raftAgreement == nullptr) {
                return true;
            }
            return raftAgreement->ready();
        }

    private:
        bool isRaftLeader = false;
        int localGroupId = -1;
        int lastBlockId = -1;
        std::unique_ptr<rb::RoundBasedAgreement> raftAgreement;
        std::shared_ptr<v2::RaftCallback> agreementCallback;
    };
}