//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/local_distributor.h"
#include "peer/consensus/block_order/async_agreement.h"

namespace peer::consensus::v2 {
    class OrderACB : public AsyncAgreementCallback {
    public:
        explicit OrderACB(std::function<bool(int chainId, int blockNumber)> deliverCallback) {
            // called by multiple AsyncAgreement when nodes involved in raft instance, access concurrently
            onDeliverHandle = std::move(deliverCallback);
            onBroadcastHandle = [this](const std::string& decision) {
                return localDistributor->gossip(decision);
            };
        }

        void init(int groupCount, std::unique_ptr<LocalDistributor> ld) {
            localDistributor = std::move(ld);
            localDistributor->setDeliverCallback([&](const std::string& decision) {
                if (!applyRawBlockOrder(decision)) {
                    LOG(WARNING) << "receive wrong order from gossip!";
                }
            });
            AsyncAgreementCallback::init(groupCount);
        }

    protected:
        std::unique_ptr<LocalDistributor> localDistributor;
    };

    class BlockOrder {
    public:
        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &localReceivers,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &multiRaftParticipant,
                const std::vector<int> &multiRaftLeaderPos,  // the leader indexes in multiRaftParticipant
                const util::NodeConfigPtr &me,
                std::shared_ptr<OrderACB> callback) {
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
                auto ld = LocalDistributor::NewLocalDistributor(localReceivers, mePos);
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
                    auto aa = AsyncAgreement::NewAsyncAgreement(localConfig, bo->agreementCallback);
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
        bool voteNewBlock(int chainId, int blockId) {
            if (!isRaftLeader) {    // local node must be raft leader
                return false;
            }
            return raftAgreement->onLeaderVotingNewBlock(chainId, blockId);
        }

        [[nodiscard]] bool isLeader() const { return isRaftLeader; }

        // wait until the node become the leader of the raft group
        [[nodiscard]] bool waitUntilRaftReady() const {
            if (raftAgreement == nullptr) {
                return true;
            }
            return raftAgreement->ready();
        }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            agreementCallback->setBCCSPWithThreadPool(std::move(bccsp), std::move(threadPool));
        }

    private:
        bool isRaftLeader = false;
        std::unique_ptr<AsyncAgreement> raftAgreement;
        std::shared_ptr<AsyncAgreementCallback> agreementCallback;
    };
}