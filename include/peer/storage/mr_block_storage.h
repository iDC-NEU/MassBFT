//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"
#include "bthread/butex.h"
#include "common/phmap.h"
#include "common/concurrent_queue.h"
#include <shared_mutex>

namespace peer {
    // the block storage for ALL regions(include this one)
    class MRBlockStorage {
    public:
        // the return value of subscriberWaitForBlock
        // int: region id, default -1
        // std::shared_ptr<proto::Block>: default nullptr
        using SubscriberContent = std::pair<int, std::shared_ptr<proto::Block>>;

        explicit MRBlockStorage(int regionCount)
                : blockStorage(regionCount)
                , newBlockFutexList(regionCount) {
            for (int i = 0; i < regionCount; i++) {
                newBlockFutexList[i] = bthread::butex_create_checked<butil::atomic<int>>();
                newBlockFutexList[i]->store(-1, std::memory_order_relaxed);
            }
        }

        ~MRBlockStorage() {
            for (int i = 0; i < (int)blockStorage.size(); i++) {
                bthread::butex_destroy(newBlockFutexList[i]);
            }
        }

        MRBlockStorage(const MRBlockStorage &) = delete;

        MRBlockStorage(MRBlockStorage &&) = delete;

        [[nodiscard]] auto regionCount() const { return blockStorage.size(); }

        // thread safe, return -1 if not exist
        [[nodiscard]] int getMaxStoredBlockNumber(int regionId) const {
            return newBlockFutexList[regionId]->load(std::memory_order_acquire);
        }

        [[nodiscard]] int newSubscriber() {
            std::unique_lock lock(mutex);
            auto id = (int)subscriberList.size();
            subscriberList.push_back(std::make_unique<util::BlockingConcurrentQueue<SubscriberContent>>());
            return id;
        }

        // timeoutMs == 0, try dequeue
        // timeoutMs < 0, wait dequeue
        // timeoutMs > 0, wait dequeue timed
        SubscriberContent subscriberWaitForBlock(int subscriberId, int timeoutMs) {
            std::shared_lock lock(mutex);
            if (subscriberId >= (int)subscriberList.size() || subscriberId < 0) {
                return {-1, nullptr};
            }
            SubscriberContent block{};
            if (timeoutMs == 0) {
                subscriberList[subscriberId]->try_dequeue(block);
                return block;
            }
            if (timeoutMs < 0) {
                subscriberList[subscriberId]->wait_dequeue(block);
                return block;
            }
            // timeoutMs > 0
            subscriberList[subscriberId]->wait_dequeue_timed(block, std::chrono::milliseconds(timeoutMs));
            return block;
        }

        // blockId start at 0
        // thread safe, nullptr if not exist
        std::shared_ptr<proto::Block> waitForBlock(int regionId, proto::BlockNumber blockId, int timeoutMs=-1) {
            if ((int) blockStorage.size() <= regionId) {
                return nullptr;
            }
            timespec timeoutSpec{};
            timespec* timeoutSpecPtr = nullptr;
            if (timeoutMs > 0) {
                timeoutSpec = butil::milliseconds_to_timespec(timeoutMs);
                timeoutSpecPtr = &timeoutSpec;
            }
            auto& futex = newBlockFutexList[regionId];
            auto maxBlockId = futex->load(std::memory_order_relaxed);
            while (maxBlockId < (int) blockId) {
                if (bthread::butex_wait(futex, maxBlockId, timeoutSpecPtr) < 0 && errno != EWOULDBLOCK && errno != EINTR) {
                    return nullptr;
                }
                maxBlockId = futex->load(std::memory_order_relaxed);
            }
            std::shared_ptr<proto::Block> block = nullptr;
            blockStorage[regionId].if_contains(blockId, [&block](const RegionStorage::value_type &v) { block = v.second.block; });
            DCHECK(block != nullptr) << "Block must not be empty, impl error!";
            return block;
        }

        // thread safe, insert a block and notify all subscribers
        void insertBlockAndNotify(int regionId, std::shared_ptr<proto::Block> block) {
            if ((int) blockStorage.size() <= regionId) {
                return;
            }
            {   // notify all consumers
                std::shared_lock lock(mutex);
                for (auto& it: subscriberList) {
                    it->enqueue({regionId, block});
                }
            }
            auto blockNumber = block->header.number;
            blockStorage[regionId].try_emplace(blockNumber, BlockCell{std::move(block)});
            auto blockNumberInt = (int) blockNumber;
            auto& futex = newBlockFutexList[regionId];
            futex->store(blockNumberInt);
            bthread::butex_wake_all(futex);
        }

    private:
        struct BlockCell {
            // the deserialized block and the related raw form
            std::shared_ptr<proto::Block> block = nullptr;
        };
        // key block id, value actual block
        using RegionStorage = util::MyFlatHashMap<proto::BlockNumber, BlockCell, std::mutex>;
        // multi region block storage
        std::vector<RegionStorage> blockStorage;
        // change when block updated
        std::vector<butil::atomic<int>*> newBlockFutexList;
        std::shared_mutex mutex;
        std::vector<std::unique_ptr<util::BlockingConcurrentQueue<SubscriberContent>>> subscriberList;
    };
}