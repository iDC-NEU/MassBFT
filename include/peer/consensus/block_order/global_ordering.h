//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/local_distributor.h"
#include "peer/consensus/block_order/async_agreement.h"
#include "common/concurrent_queue.h"

namespace peer::consensus::v2 {
    class OrderACB : public AsyncAgreementCallback {
    public:
        OrderACB(int groupCount, std::unique_ptr<LocalDistributor> ld, auto &&cb)
                : AsyncAgreementCallback(groupCount),
                  localDistributor(std::move(ld)) {
            onDeliver = std::forward<decltype(cb)>(cb);
            localDistributor->setDeliverCallback([&](const std::string& decision) {
                if (!applyRawBlockOrder(decision)) {
                    LOG(WARNING) << "receive wrong order from gossip!";
                }
            });
        }

        // called by multiple AsyncAgreement when nodes involved in raft instance, access concurrently
        bool onBroadcast(std::string decision) override {
            return localDistributor->gossip(decision);
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
                std::function<bool(int, int)> onDeliver) {
            auto bo = std::make_unique<BlockOrder>();
            {   // init LocalDistributor
                // find local nodes position
                int mePos = -1;
                for (int i = 0; i < (int) localReceivers.size(); i++) {
                    if (localReceivers[i]->nodeConfig == me) {
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
                bo->agreementCallback = std::make_shared<OrderACB>(maxGroupId + 1, std::move(ld), std::move(onDeliver));
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

    private:
        bool isRaftLeader = false;
        std::unique_ptr<AsyncAgreement> raftAgreement;
        std::shared_ptr<AsyncAgreementCallback> agreementCallback;
    };
}