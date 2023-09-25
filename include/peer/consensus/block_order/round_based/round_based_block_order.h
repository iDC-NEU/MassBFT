//
// Created by user on 23-9-14.
//

#pragma once

#include "peer/consensus/block_order/round_based/round_based_agreement.h"
#include "peer/consensus/block_order/round_based/round_based_order_manager.h"
#include "peer/consensus/block_order/block_order.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"

namespace peer::consensus::rb {
    class RaftLogValidator {
    public:
        explicit RaftLogValidator(std::shared_ptr<::peer::MRBlockStorage> storage)
                : _storage(std::move(storage)) {
            if (_storage == nullptr) {
                LOG(WARNING) << "Storage is empty, validator may not wait until receiving the actual block.";
            }
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

    private:
        std::shared_ptr<::peer::MRBlockStorage> _storage;
    };

    class RoundBasedCallback : public v2::RaftCallback {
    public:
        RoundBasedCallback(std::unique_ptr<RaftLogValidator> validator)
                :_validator(std::move(validator)) {
            setOnErrorCallback([this](int subChainId) {
                return _orderManager->invalidateChain(subChainId);
            });

            setOnValidateCallback([this](const std::string& decision)->bool {
                if (_validator != nullptr) {
                    return _validator->waitUntilReceiveValidBlock(decision);
                }
                return true;
            });

            setOnBroadcastCallback([this](const std::string& decision)->bool {
                return applyRawBlockOrder(decision);
            });
        }

        void init(int groupCount, std::unique_ptr<v2::LocalDistributor> ld) override {
            v2::RaftCallback::init(groupCount, std::move(ld));
            auto om = std::make_unique<RoundBasedOrderManager>();
            om->setSubChainCount(groupCount);
            om->setDeliverCallback([this](int chainId, int blockId) {
                if (!onExecuteBlock(chainId, blockId)) {
                    LOG(ERROR) << "Execute block failed, bid: " << blockId;
                }
            });
            _orderManager = std::move(om);
        }

    protected:
        bool applyRawBlockOrder(const std::string& decision) {
            int chainId, blockId;
            zpp::bits::in in(decision);
            if(failure(in(chainId, blockId))) {
                return false;
            }
            return _orderManager->pushDecision(chainId, blockId);
        }

    private:
        std::unique_ptr<RoundBasedOrderManager> _orderManager;
        std::unique_ptr<RaftLogValidator> _validator;
    };

    class BlockOrder : public BlockOrderInterface {
    public:
        static std::unique_ptr<v2::RaftCallback> NewRaftCallback(std::shared_ptr<::peer::MRBlockStorage> storage,
                                                                 const std::shared_ptr<util::BCCSP>&,
                                                                 const std::shared_ptr<util::thread_pool_light>&) {
            auto validator = std::make_unique<RaftLogValidator>(std::move(storage));
            return std::make_unique<RoundBasedCallback>(std::move(validator));
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
                    auto aa = RoundBasedAgreement::NewRoundBasedAgreement(localConfig, bo->agreementCallback);
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
                    bo->raftAgreement = std::move(aa);
                }
            }
            return bo;
        }

        // only raft leader can invoke this function
        bool voteNewBlock(int chainId, int blockId) override {
            if (!isRaftLeader) {    // local node must be raft leader
                return false;
            }    // leader only consensus block generate by this group
            return raftAgreement->onReplicatingNewBlock(chainId, blockId);
        }

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
        std::unique_ptr<RoundBasedAgreement> raftAgreement;
        std::shared_ptr<v2::RaftCallback> agreementCallback;
    };
}