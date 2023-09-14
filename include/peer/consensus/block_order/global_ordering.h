//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/async_agreement.h"
#include "peer/consensus/block_order/block_order.h"

namespace peer::consensus::v2 {
    class OrderACB : public RaftCallback {
    public:
        OrderACB() {
            setOnErrorCallback([this](int subChainId) {
                // the group is down, invalid all the block
                // _orderManager->invalidateChain(subChainId);
                proto::BlockOrder bo {
                        .chainId = subChainId,
                        .blockId = -1,
                        .voteChainId = -1,
                        .voteBlockId = -1
                };
                ::proto::SignedBlockOrder sb;
                if (!bo.serializeToString(&sb.serializedBlockOrder)) {
                    return;
                }
                std::string buffer;
                sb.serializeToString(&buffer);
                // broadcast to all nodes in this group
                onBroadcast(std::move(buffer));
            });

            setOnBroadcastCallback([this](const std::string& decision)->bool {
                return applyRawBlockOrder(decision);
            });
        }

        void init(int groupCount, std::unique_ptr<LocalDistributor> ld) override {
            RaftCallback::init(groupCount, std::move(ld));
            auto om = std::make_unique<v2::InterChainOrderManager>();
            om->setSubChainCount(groupCount);
            om->setDeliverCallback([this](const v2::InterChainOrderManager::Cell* c) {
                // return the final decision to caller
                if (!onExecuteBlock(c->subChainId, c->blockNumber)) {
                    LOG(ERROR) << "Execute block failed, bid: " << c->blockNumber;
                }
            });
            // Optimize-1: order next block as soon as receiving the previous block
            // for (int i=0; i<groupCount; i++) {
            //     for (int j=0; j<groupCount; j++) {
            //         CHECK(om->pushDecision(i, 0, { j, -1 }));
            //     }
            // }
            _orderManager = std::move(om);
        }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _threadPoolForBCCSP = std::move(threadPool);
        }

    protected:
        bool applyRawBlockOrder(const std::string& decision) {
            proto::SignedBlockOrder sb;
            if (!sb.deserializeFromString(decision)) {
                return false;
            }
            if (!validateSignatureOfBlockOrder(sb)) {
                return false;
            }
            proto::BlockOrder bo{};
            if (!bo.deserializeFromString(sb.serializedBlockOrder)) {
                return false;
            }
            if (bo.voteChainId == -1) {   // this is an error message
                CHECK(bo.blockId == -1 && bo.voteBlockId == -1);
                // the group is down, invalid all the block
                return _orderManager->invalidateChain(bo.chainId);
            }
            return _orderManager->pushDecision(bo.chainId, bo.blockId, { bo.voteChainId, bo.voteBlockId });
            // Optimize-1: order next block as soon as receiving the previous block
            // return _orderManager->pushDecision(bo.chainId, bo.blockId + 1, { bo.voteChainId, bo.voteBlockId + 1 });
        }

        bool validateSignatureOfBlockOrder(const proto::SignedBlockOrder& sb) {
            if (sb.signatures.empty()) {    // optimize
                // DLOG(WARNING) << "Sigs are empty in validateSignatureOfBlockOrder!";
                return true;
            }
            bool success = true;
            auto numRoutines = (int)_threadPoolForBCCSP->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPoolForBCCSP->push_emergency_task([&, start=i] {
                    auto& payload = sb.serializedBlockOrder;
                    for (int j = start; j < (int)sb.signatures.size(); j += numRoutines) {
                        auto& signature = sb.signatures[j];
                        const auto key = _bccsp->GetKey(signature.ski);
                        if (key == nullptr) {
                            LOG(WARNING) << "Can not load key, ski: " << signature.ski;
                            success = false;
                            break;
                        }
                        if (!key->Verify(signature.digest, payload.data(), payload.size())) {
                            success = false;
                            break;
                        }
                    }
                    countdown.signal();
                });
            }
            countdown.wait();
            return success;
        }

    private:
        std::unique_ptr<v2::InterChainOrderManager> _orderManager;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
    };

    class BlockOrder : public BlockOrderInterface {
    public:
        static std::unique_ptr<RaftCallback> NewRaftCallback(std::shared_ptr<util::BCCSP> bccsp,
                                                             std::shared_ptr<util::thread_pool_light> threadPool) {
            auto acb = std::make_unique<peer::consensus::v2::OrderACB>();
            acb->setBCCSPWithThreadPool(std::move(bccsp), std::move(threadPool));
            return acb;
        }

        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &localReceivers,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &multiRaftParticipant,
                const std::vector<int> &multiRaftLeaderPos,  // the leader indexes in multiRaftParticipant
                const util::NodeConfigPtr &me,
                std::shared_ptr<RaftCallback> callback) {
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
        bool voteNewBlock(int chainId, int blockId) override {
            if (!isRaftLeader) {    // local node must be raft leader
                return false;
            }
            return raftAgreement->onLeaderVotingNewBlock(chainId, blockId);
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
        std::unique_ptr<AsyncAgreement> raftAgreement;
        std::shared_ptr<RaftCallback> agreementCallback;
    };
}