//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"
#include "bthread/butex.h"
#include "common/phmap.h"

namespace peer {

    // the block storage for ALL regions(include this one)
    class MRBlockStorage {
    public:
        explicit MRBlockStorage(int regionCount)
                : blockStorage(regionCount)
                , newBlockFutexList(regionCount)
                , persistBlockFutexList(regionCount) {
            for (int i = 0; i < regionCount; i++) {
                newBlockFutexList[i] = bthread::butex_create_checked<butil::atomic<int>>();
                newBlockFutexList[i]->store(-1, std::memory_order_relaxed);
                persistBlockFutexList[i] = bthread::butex_create_checked<butil::atomic<int>>();
                persistBlockFutexList[i]->store(-1, std::memory_order_relaxed);
            }
            totalNewBlockCount = bthread::butex_create_checked<butil::atomic<int>>();
            totalPersistBlockCount = bthread::butex_create_checked<butil::atomic<int>>();
        }

        virtual ~MRBlockStorage() {
            for (int i = 0; i < (int)blockStorage.size(); i++) {
                bthread::butex_destroy(newBlockFutexList[i]);
                bthread::butex_destroy(persistBlockFutexList[i]);
            }
            bthread::butex_destroy(totalNewBlockCount);
            bthread::butex_destroy(totalPersistBlockCount);
        }

        MRBlockStorage(const MRBlockStorage &) = delete;

        MRBlockStorage(MRBlockStorage &&) = delete;

        // thread safe, return -1 if not exist
        int getMaxStoredBlockNumber(int regionId) {
            return newBlockFutexList[regionId]->load(std::memory_order_acquire);
        }

        // thread safe, nullptr if not exist
        std::shared_ptr<proto::Block> getBlock(int regionId, proto::BlockNumber blockId) {
            if ((int) blockStorage.size() <= regionId) {
                return nullptr;
            }
            std::shared_ptr<proto::Block> block = nullptr;
            blockStorage[regionId].if_contains(blockId, [&block](const RegionStorage::value_type &v) { block = v.second.block; });
            return block;
        }

        // thread safe
        void insertBlock(int regionId, std::shared_ptr<proto::Block> block) {
            if ((int) blockStorage.size() <= regionId) {
                return;
            }
            auto blockNumber = block->header.number;
            blockStorage[regionId].try_emplace(blockNumber, BlockCell{false, std::move(block)});
        }

        // thread safe, return false if not exist
        bool setPersist(int regionId, proto::BlockNumber blockId) {
            if ((int) blockStorage.size() <= regionId) {
                return false;
            }
            blockStorage[regionId].modify_if(blockId, [](RegionStorage::value_type &v) { v.second.persist = true; });
            return true;
        }

        // return true if persisted
        bool isPersist(int regionId, proto::BlockNumber blockId) {
            if ((int) blockStorage.size() <= regionId) {
                return false;
            }
            bool ret = false;
            blockStorage[regionId].if_contains(blockId, [&ret](const RegionStorage::value_type &v) { ret = v.second.persist; });
            return ret;
        }

        [[nodiscard]] auto regionCount() const { return blockStorage.size(); }

        int onReceivedNewBlock(int regionId, proto::BlockNumber blockNumber) {
            auto smallBlockNumber = (int) blockNumber;
            auto& futex = newBlockFutexList[regionId];
            futex->store(smallBlockNumber);
            return bthread::butex_wake_all(futex);
        }

        // blockNumber is the maximum processed block
        // oldBlockNumber == -1 on starting
        // auto timeout = butil::milliseconds_to_timespec(1000);
        bool waitForNewBlock(int regionId, int nextBlockNumber, const timespec* timeout) {
            auto& futex = newBlockFutexList[regionId];
            bthread::butex_wait(futex, nextBlockNumber-1, timeout);
            return futex->load(std::memory_order_relaxed) >= nextBlockNumber;
        }

        int onBlockPersist(int regionId, proto::BlockNumber blockNumber) {
            auto smallBlockNumber = (int) blockNumber;
            auto& futex = persistBlockFutexList[regionId];
            futex->store(smallBlockNumber);
            return bthread::butex_wake_all(futex);
        }

        // blockNumber is the maximum processed block
        // oldBlockNumber == -1 on starting
        // auto timeout = butil::milliseconds_to_timespec(1000);
        bool waitForBlockPersist(int regionId, int nextBlockNumber, const timespec* timeout) {
            auto& futex = persistBlockFutexList[regionId];
            bthread::butex_wait(futex, nextBlockNumber-1, timeout);
            return futex->load(std::memory_order_relaxed) >= nextBlockNumber;
        }

        bool onReceivedNewBlock() {
            totalNewBlockCount->fetch_add(1, std::memory_order_relaxed);
            return bthread::butex_wake_all(totalNewBlockCount);
        }

        // currentBlockCount == 0 on starting
        // auto timeout = butil::milliseconds_to_timespec(1000);
        int waitForNewBlock(int currentBlockCount, const timespec* timeout) {
            bthread::butex_wait(totalNewBlockCount, currentBlockCount, timeout);
            return totalNewBlockCount->load(std::memory_order_relaxed) > currentBlockCount;
        }

        int onBlockPersist() {
            totalPersistBlockCount->fetch_add(1, std::memory_order_relaxed);
            return bthread::butex_wake_all(totalPersistBlockCount);
        }

        // currentBlockCount == 0 on starting
        // auto timeout = butil::milliseconds_to_timespec(1000);
        int waitForBlockPersist(int currentBlockCount, const timespec* timeout) {
            bthread::butex_wait(totalPersistBlockCount, currentBlockCount, timeout);
            return totalNewBlockCount->load(std::memory_order_relaxed) > currentBlockCount;
        }

    private:
        struct BlockCell {
            // block replicated successfully over n/2 regions
            bool persist = false;
            // the deserialized block and the related raw form
            std::shared_ptr<proto::Block> block = nullptr;
        };
        // key block id, value actual block
        using RegionStorage = util::MyFlatHashMap<proto::BlockNumber, BlockCell, std::mutex>;
        // multi region block storage
        std::vector<RegionStorage> blockStorage;
        // change when block updated
        butil::atomic<int>* totalNewBlockCount;
        butil::atomic<int>* totalPersistBlockCount;
        std::vector<butil::atomic<int>*> newBlockFutexList;
        std::vector<butil::atomic<int>*> persistBlockFutexList;
    };
}