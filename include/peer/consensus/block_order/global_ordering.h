//
// Created by user on 23-5-7.
//

#include "peer/consensus/block_order/async_agreement.h"
#include "peer/consensus/block_order/block_order.h"
#include "peer/consensus/block_order/interchain_order_manager.h"
#include "peer/storage/mr_block_storage.h"

namespace peer::consensus::v2 {
    class RaftLogValidator {
    public:
        RaftLogValidator(std::shared_ptr<::peer::MRBlockStorage> storage,
                         std::shared_ptr<util::BCCSP> bccsp,
                         std::shared_ptr<util::thread_pool_light> threadPool)
                : _storage(std::move(storage)), _bccsp(std::move(bccsp)), _threadPool(std::move(threadPool)) {
            if (_storage == nullptr) {
                LOG(WARNING) << "Storage is empty, validator may not wait until receiving the actual block.";
            }
        }

        [[nodiscard]] bool waitUntilReceiveValidBlock(const proto::BlockOrder& bo, int timeoutMs) const {
            if (_storage == nullptr) {
                return true;    // skip waiting block
            }
            if (_storage->waitForBlock(bo.chainId, bo.blockId, timeoutMs) == nullptr) {
                return false;
            }
            return true;
        }

        [[nodiscard]] bool validateSignatureOfBlockOrder(const proto::SignedBlockOrder& sb) const {
            if (sb.signatures.empty()) {    // optimize
                // DLOG(WARNING) << "Sigs are empty in validateSignatureOfBlockOrder!";
                return true;
            }
            bool success = true;
            auto numRoutines = (int)_threadPool->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPool->push_emergency_task([&, start=i] {
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
        std::shared_ptr<::peer::MRBlockStorage> _storage;
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPool;
    };

    class OrderACB : public RaftCallback {
    public:
        explicit OrderACB(std::unique_ptr<RaftLogValidator> validator)
                :_validator(std::move(validator)) {
            _getBlockVoteCountCallback = [](int, int){ return true; };
            _increaseBlockVoteCallback = [](int, int, int){ return true; };
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

            // concurrent access
            setOnValidateCallback([this](const std::string& decision)->bool {
                if (_validator != nullptr) {
                    proto::SignedBlockOrder sb{};
                    if (!sb.deserializeFromString(decision)) {
                        return false;
                    }
                    proto::BlockOrder bo{};
                    if (!bo.deserializeFromString(sb.serializedBlockOrder)) {
                        return false;
                    }
                    if (bo.voteChainId == -1) {
                        return true;    // this is a view-change message
                    }
                    if (!_increaseBlockVoteCallback(bo.chainId, bo.blockId, bo.voteChainId)) {
                        return false;   // add count
                    }
                    if (_getBlockVoteCountCallback(bo.chainId, bo.blockId)) {    // received f+1 votes
                        return true;
                    }
                    if (!_validator->validateSignatureOfBlockOrder(sb)) {
                        return false;   // signature checksum error
                    }
                    for (int i=0; i<20; i++) {
                        if (_validator->waitUntilReceiveValidBlock(bo, 10)) { // received actual block
                            return true;
                        }
                        if (_getBlockVoteCountCallback(bo.chainId, bo.blockId)) {    // received f+1 votes
                            return true;
                        }
                    }
                    LOG(ERROR) << "I can not receive block after 20 tries: " << bo.chainId << ", " << bo.blockId;
                    return false;
                }
                return true;
            });

            setOnBroadcastCallback([this](const std::string& decision)->bool {
                return applyRawBlockOrder(decision);
            });
        }

        // initialized by BlockOrder::NewBlockOrder
        void init(int groupCount, std::unique_ptr<LocalDistributor> ld) override {
            RaftCallback::init(groupCount, std::move(ld));
            auto om = std::make_unique<v2::InterChainOrderManager>();
            om->setGroupCount(groupCount);
            om->setDeliverCallback([this](const v2::InterChainOrderManager::Cell* c) {
                // use to test if all nodes runs in the same order
                // static int idx = 0;
                // static auto gid = std::this_thread::get_id();
                // LOG(INFO) << "Node " << gid << " execute " << idx ++ << " " << c->groupId << " " << c->blockId;

                // return the final decision to caller
                if (!onExecuteBlock(c->groupId, c->blockId)) {
                    LOG(ERROR) << "Execute block failed, bid: " << c->blockId;
                }
            });
            _orderManager = std::move(om);
        }

        // set by NewBlockOrder, the group leader in its Raft group receive f+1 votes, increase its vector clock
        void setIncreaseVCCallback(auto&& cb) { _increaseVCCallback = std::forward<decltype(cb)>(cb); }

        // the group leader receive remote vc for a block, increase total vc count
        // return true on success, false when increase error
        void setIncreaseBlockVoteCallback(auto&& cb) { _increaseBlockVoteCallback = std::forward<decltype(cb)>(cb); }

        // have more than f+1 votes to proceed?
        void setGetBlockVoteCountCallback(auto&& cb) { _getBlockVoteCountCallback = std::forward<decltype(cb)>(cb); }

    protected:
        bool applyRawBlockOrder(const std::string& decision) {
            proto::SignedBlockOrder sb;
            if (!sb.deserializeFromString(decision)) {
                return false;
            }
            proto::BlockOrder bo{};
            if (!bo.deserializeFromString(sb.serializedBlockOrder)) {
                return false;
            }
            if (bo.voteChainId == -1) {   // this is an error message
                CHECK(bo.blockId == -1 && bo.voteBlockId == -1);
                // the group is down, invalid all the block
                _orderManager->invalidateChain(bo.chainId);
                return true;
            }
            // if is leader, increase local vc
            if (_increaseVCCallback) {
                _increaseVCCallback(bo.chainId, bo.blockId);
            }
            // LOG(INFO)  << "DEBUG  " << bo.chainId << ", " << bo.blockId << ", " << bo.voteChainId << ", " <<bo.voteBlockId;
            _orderManager->pushDecision(bo.chainId, bo.blockId,  bo.voteChainId, bo.voteBlockId);
            return true;
        }

    private:
        std::unique_ptr<v2::InterChainOrderManager> _orderManager;
        std::unique_ptr<RaftLogValidator> _validator;
        std::function<bool(int chainId, int blockId)> _increaseVCCallback;
        std::function<bool(int chainId, int blockId, int voteChainId)> _increaseBlockVoteCallback;
        std::function<bool(int chainId, int blockId)> _getBlockVoteCountCallback;
    };

    class BlockOrder : public BlockOrderInterface {
    public:
        static std::unique_ptr<OrderACB> NewRaftCallback(std::shared_ptr<::peer::MRBlockStorage> storage,
                                                         std::shared_ptr<util::BCCSP> bccsp,
                                                         std::shared_ptr<util::thread_pool_light> threadPool) {
            auto validator = std::make_unique<RaftLogValidator>(std::move(storage), std::move(bccsp), std::move(threadPool));
            auto acb = std::make_unique<OrderACB>(std::move(validator));
            return acb;
        }

        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &localReceivers,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &multiRaftParticipant,
                const std::vector<int> &multiRaftLeaderPos,  // the leader indexes in multiRaftParticipant
                const util::NodeConfigPtr &me,
                const std::shared_ptr<OrderACB>& callback) {
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
                bo->agreementCallback = callback;   // do not move it, use later
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
                if (localConfig != nullptr) {
                    // init order assigner
                    auto orderAssigner = std::make_unique<v2::OrderAssigner>();
                    orderAssigner->setLocalChainId(localConfig->nodeConfig->groupId);
                    bo->_orderAssigner = std::move(orderAssigner);
                    // init aa
                    auto aa = AsyncAgreement::NewAsyncAgreement(localConfig, callback);
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
                    if (bo->isRaftLeader) {
                        callback->setIncreaseVCCallback([ptr = bo.get()](int chainId, int blockId) -> bool {
                            return ptr->_orderAssigner->increaseLocalClock(chainId, blockId);
                        });
                        // get the current votes of the block
                        callback->setGetBlockVoteCountCallback(
                                [ptr = bo.get(), groupCount = callback->getGroupCount(), localGroupId = localConfig->nodeConfig->groupId]
                                        (int chainId, int blockId) -> bool {
                                    const auto minThreshHold = groupCount / 2 + 1;    // more than half
                                    auto count = ptr->_orderAssigner->getVoteForBlock(chainId, blockId);
                                    CHECK(groupCount >= count);
                                    if (count < 0) {    // already finished
                                        return true;
                                    }
                                    return count >= minThreshHold;  // > f+1?
                                }
                        );

                        // force the current leader voting the block
                        callback->setIncreaseBlockVoteCallback(
                                [ptr = bo.get(), groupCount = callback->getGroupCount(), localGroupId = localConfig->nodeConfig->groupId]
                                        (int chainId, int blockId, int voteChainId) -> bool {
                                    auto count = ptr->_orderAssigner->addVoteForBlock(chainId, blockId, voteChainId);
                                    const auto minThreshHold = groupCount / 2 + 1;    // more than half
                                    CHECK(groupCount >= count);
                                    if (count != minThreshHold) {
                                        return true;
                                    }
                                    // ensure only invoke once == f + 1
                                    auto success = ptr->voteNewBlock(chainId, blockId);
                                    if (success) {
                                        LOG(INFO) << "Leader of group " << localGroupId << " force vote new block " << chainId << ", " << blockId << ", " << count;
                                    }
                                    return success;
                                }
                        );
                    }
                }
            }
            return bo;
        }

        // only raft leader can invoke this function
        bool voteNewBlock(int chainId, int blockId) override {
            if (!isRaftLeader) {    // local node must be raft leader
                return false;
            }
            auto localVC = _orderAssigner->getBlockOrder(chainId, blockId);
            if (localVC.first == -1 && localVC.second == -1) {
                return true;    // already voted
            }
            // LOG(INFO) << "Leader of group " << chainId << " vote new block " << chainId << ", " << blockId;
            proto::BlockOrder bo {
                    .chainId = chainId,
                    .blockId = blockId,
                    .voteChainId = localVC.first,
                    .voteBlockId = localVC.second
            };
            return raftAgreement->onLeaderVotingNewBlock(bo);
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
        std::unique_ptr<v2::OrderAssigner> _orderAssigner;
        std::unique_ptr<AsyncAgreement> raftAgreement;
        std::shared_ptr<RaftCallback> agreementCallback;
    };
}