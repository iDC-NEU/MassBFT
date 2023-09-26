//
// Created by user on 23-9-14.
//

#pragma once

#include "peer/consensus/block_order/block_order.h"
#include "peer/consensus/block_order/local_distributor.h"
#include "peer/storage/mr_block_storage.h"
#include "common/thread_pool_light.h"
#include "common/bccsp.h"

namespace peer::consensus::geobft {
    class RaftLogValidator : public v2::RaftCallback {
    public:
        explicit RaftLogValidator(std::shared_ptr<::peer::MRBlockStorage> storage)
                : _storage(std::move(storage)) {
            CHECK(_storage != nullptr) << "Storage is empty, validator may not wait until receiving the actual block.";
            _id = _storage->newSubscriber();
        }

        void init(int groupCount, std::unique_ptr<v2::LocalDistributor> ld) override {
            v2::RaftCallback::init(groupCount, std::move(ld));
            _orderThread = std::make_unique<std::thread>(&RaftLogValidator::orderBlock, this);
        }

        ~RaftLogValidator() override {
            _running = false;
            if (_orderThread) {
                _orderThread->join();
            }
        }

        void orderBlock() {
            CHECK(_id != -1);
            std::vector<int> blockList(getGroupCount());
            for (auto& it: blockList) {
                it = -1;
            }
            while(_running) {
                auto blockPair = _storage->subscriberWaitForBlock(_id, 500);
                if (blockPair.second == nullptr) {
                    continue;
                }
                auto& previousId = blockList[blockPair.first];
                CHECK(previousId == (int)blockPair.second->header.number - 1);
                previousId++;
                bool canExecute = true;
                for (auto& it: blockList) {
                    if (it < previousId) {
                        canExecute = false;
                        break;
                    }
                }
                if (!canExecute) {
                    continue;   // wait for more block
                }
                for (int i=0; i<(int)blockList.size(); i++) {
                    onExecuteBlock(i, previousId);  // execute all the block
                }
            }
        }

        [[nodiscard]] auto getStorage() const {
            return _storage;
        }

    private:
        int _id = -1;
        std::atomic<bool> _running = true;
        std::shared_ptr<::peer::MRBlockStorage> _storage;
        std::unique_ptr<std::thread> _orderThread;
    };

    class BlockOrder : public BlockOrderInterface {
    public:
        static std::unique_ptr<v2::RaftCallback> NewRaftCallback(std::shared_ptr<::peer::MRBlockStorage> storage,
                                                                 const std::shared_ptr<util::BCCSP>&,
                                                                 const std::shared_ptr<util::thread_pool_light>&) {
            auto validator = std::make_unique<RaftLogValidator>(std::move(storage));
            return validator;
        }

        // I may not exist in multiRaftParticipant, but must exist in localReceivers
        static std::unique_ptr<BlockOrder> NewBlockOrder(
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>&,
                const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &multiRaftParticipant,
                const std::vector<int>&,  // the leader indexes in multiRaftParticipant
                const util::NodeConfigPtr&,
                std::shared_ptr<v2::RaftCallback> callback) {
            auto bo = std::make_unique<BlockOrder>();
            int maxGroupId = 0;
            for (auto &it: multiRaftParticipant) {
                maxGroupId = std::max(maxGroupId, it->nodeConfig->groupId);
            }
            auto ld = v2::LocalDistributor::NewLocalDistributor({}, -1);
            callback->init(maxGroupId + 1, std::move(ld));
            bo->agreementCallback = std::move(callback);
            return bo;
        }

        // only raft leader can invoke this function
        bool voteNewBlock(int, int) override {
            return true;
        }

        [[nodiscard]] bool isLeader() const override { return false; }

        // wait until the node become the leader of the raft group
        [[nodiscard]] bool waitUntilRaftReady() const override {
            return true;
        }

    private:
        std::shared_ptr<v2::RaftCallback> agreementCallback;
    };
}