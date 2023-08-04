//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"
#include "bthread/butex.h"
#include "common/phmap.h"
#include "common/lru.h"
#include "common/concurrent_queue.h"
#include <shared_mutex>

namespace peer {

    namespace inner {
        template<class Derived>
        class BlockStorageBase {
        public:
            using SubscriberContent = std::pair<int, std::shared_ptr<proto::Block>>;
            // the return value of subscriberWaitForBlock
            // int: region id, default -1
            // std::shared_ptr<proto::Block>: default nullptr
            explicit BlockStorageBase(int regionCount)
                    : newBlockFutexList(regionCount) {
                for (int i = 0; i < regionCount; i++) {
                    newBlockFutexList[i] = bthread::butex_create_checked<butil::atomic<int>>();
                    newBlockFutexList[i]->store(-1, std::memory_order_relaxed);
                }
            }

            ~BlockStorageBase() {
                for (auto& i : newBlockFutexList) {
                    bthread::butex_destroy(i);
                }
            }

            BlockStorageBase(const BlockStorageBase &) = delete;

            BlockStorageBase(BlockStorageBase &&) = delete;

            [[nodiscard]] auto regionCount() const { return newBlockFutexList.size(); }

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
                if ((int) newBlockFutexList.size() <= regionId) {
                    return nullptr;
                }
                timespec timeoutSpec{};
                timespec* timeoutSpecPtr = nullptr;
                if (timeoutMs > 0) {
                    timeoutSpec = butil::milliseconds_from_now(timeoutMs);
                    timeoutSpecPtr = &timeoutSpec;
                }
                auto& futex = newBlockFutexList[regionId];
                auto maxBlockId = futex->load(std::memory_order_acquire);
                while (maxBlockId < (int) blockId) {
                    if (bthread::butex_wait(futex, maxBlockId, timeoutSpecPtr) < 0 && errno != EWOULDBLOCK && errno != EINTR) {
                        return nullptr;
                    }
                    maxBlockId = futex->load(std::memory_order_acquire);
                }
                return static_cast<Derived*>(this)->getBlock(regionId, blockId);
            }

            // thread safe, insert a block and notify all subscribers
            void insertBlockAndNotify(int regionId, std::shared_ptr<proto::Block> block) {
                if ((int) newBlockFutexList.size() <= regionId) {
                    return;
                }
                {   // notify all consumers
                    std::shared_lock lock(mutex);
                    for (auto& it: subscriberList) {
                        it->enqueue({regionId, block});
                    }
                }
                auto blockId = block->header.number;
                if (!static_cast<Derived*>(this)->tryEmplaceBlock(regionId, blockId, std::move(block))) {
                    LOG(WARNING) << "Insert block failed: " << blockId;
                }
                auto blockIdInt = (int) blockId;
                auto& futex = newBlockFutexList[regionId];
                // double check if input is correct
                DCHECK(futex->load(std::memory_order_acquire) == blockIdInt - 1);
                futex->store(blockIdInt, std::memory_order_release);
                bthread::butex_wake_all(futex);
                // prune stale block
                static_cast<Derived*>(this)->pruneWithMaxBlockId(regionId, blockId);
            }

        private:
            // change when block updated
            std::vector<butil::atomic<int>*> newBlockFutexList;
            std::shared_mutex mutex;
            std::vector<std::unique_ptr<util::BlockingConcurrentQueue<SubscriberContent>>> subscriberList;
        };
    }

    // the block storage for ALL regions(including this one)
    class MRBlockStorage : public inner::BlockStorageBase<MRBlockStorage> {
    public:
        explicit MRBlockStorage(int regionCount, int maxSize = 64)
                : inner::BlockStorageBase<MRBlockStorage>(regionCount)
                , _maxSize(maxSize), blockStorage(regionCount) { }

        friend class inner::BlockStorageBase<MRBlockStorage>;

    protected:
        std::shared_ptr<proto::Block> getBlock(int regionId, proto::BlockNumber blockId) {
            std::shared_ptr<proto::Block> block = nullptr;
            blockStorage[regionId].if_contains(blockId, [&block](const RegionStorage::value_type &v) { block = v.second; });
            DCHECK(block != nullptr) << "Block must not be empty, impl error!";
            return block;
        }

        bool tryEmplaceBlock(int regionId, proto::BlockNumber blockId, auto&& block) {
            blockStorage[regionId].try_emplace(blockId, std::forward<decltype(block)>(block));
            return true;
        }

        void pruneWithMaxBlockId(int regionId, proto::BlockNumber blockId) {
            blockStorage[regionId].erase(blockId - _maxSize);
        }

    private:
        const int _maxSize;
        // key block id, value actual block
        using RegionStorage = util::MyFlatHashMap<proto::BlockNumber, std::shared_ptr<proto::Block>, std::mutex>;
        // multi region block storage
        std::vector<RegionStorage> blockStorage;
    };

    // the block storage for ALL regions(including this one)
    class BlockLRUCache : public inner::BlockStorageBase<BlockLRUCache> {
    public:
        explicit BlockLRUCache(int regionCount, int maxSize = 64)
                : inner::BlockStorageBase<BlockLRUCache>(regionCount)
                , blockStorage(regionCount) {
            for (auto& it: blockStorage) {
                it.init(maxSize);
            }
        }

        friend class inner::BlockStorageBase<BlockLRUCache>;

    protected:
        std::shared_ptr<proto::Block> getBlock(int regionId, proto::BlockNumber blockId) {
            std::shared_ptr<proto::Block> block = nullptr;
            if (!blockStorage[regionId].tryGetCopy(blockId, block)) {
                return nullptr;
            }
            return block;
        }

        bool tryEmplaceBlock(int regionId, proto::BlockNumber blockId, auto&& block) {
            blockStorage[regionId].insert(blockId, std::forward<decltype(block)>(block));
            return true;
        }

        void pruneWithMaxBlockId(int, proto::BlockNumber) { }

    private:
        // key block id, value actual block
        using RegionStorage = util::LRUCache<proto::BlockNumber, std::shared_ptr<proto::Block>, std::mutex>;
        // multi region block storage
        std::vector<RegionStorage> blockStorage;
    };
}