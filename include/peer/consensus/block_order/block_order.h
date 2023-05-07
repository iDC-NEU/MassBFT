//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/local_distributor.h"
#include "peer/consensus/block_order/async_agreement.h"
#include "common/concurrent_queue.h"

namespace peer::consensus::v2 {
    class OrderACB : public AsyncAgreementCallback {
    public:
        OrderACB(int groupCount, std::unique_ptr<LocalDistributor> ld)
                : AsyncAgreementCallback(groupCount), localDistributor(std::move(ld)) {
            localDistributor->setDeliverCallback([&](zmq::message_t decision) {
                if (!applyRawBlockOrder(decision.to_string())) {
                    LOG(WARNING) << "receive wrong order from gossip!";
                }
            });
        }

        // called by orderManager in base class
        bool onDeliver(int chainId, int blockNumber) override {
            return false;
        }

        // called by AsyncAgreement when nodes involved in raft instance
        bool onBroadcast(std::string decision) override {
            return localDistributor->gossip(std::move(decision));
        }

    protected:
        std::unique_ptr<LocalDistributor> localDistributor;
    };

    class BlockOrder {
    public:
        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& localReceivers,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& multiRaftParticipant,
                const util::NodeConfigPtr& me) {
            auto bo = std::make_unique<BlockOrder>();
            {   // init LocalDistributor
                // find local nodes position
                int mePos = -1;
                for (int i=0; i<(int)localReceivers.size(); i++) {
                    if (localReceivers[i]->nodeConfig == me) {
                        mePos = i;
                        break;
                    }
                }
                if (mePos == -1) {  // not exist
                    return nullptr;
                }
                auto ld = LocalDistributor::NewLocalDistributor(localReceivers, mePos);
                if (ld == nullptr) {
                    return nullptr;
                }
                int maxGroupId = 0;
                for (auto& it: multiRaftParticipant) {
                    maxGroupId = std::max(maxGroupId, it->nodeConfig->groupId);
                }
                bo->agreementCallback = std::make_shared<OrderACB>(maxGroupId+1, std::move(ld));
            }
            {   // init AsyncAgreement
                //---- get group count and local instance ID
                std::shared_ptr<util::ZMQInstanceConfig> localConfig = nullptr;
                for (auto& it: multiRaftParticipant) {
                    if (me == it->nodeConfig) {
                        localConfig = it;
                    }
                }
                if (localConfig != nullptr) {   // init aa
                    auto aa = AsyncAgreement::NewAsyncAgreement(localConfig, bo->agreementCallback);
                    if (aa == nullptr) {
                        return nullptr;
                    }
                    bo->raftAgreement = std::move(aa);
                }
            }

        }

    private:
        std::unique_ptr<AsyncAgreement> raftAgreement;
        std::shared_ptr<AsyncAgreementCallback> agreementCallback;

    };
}