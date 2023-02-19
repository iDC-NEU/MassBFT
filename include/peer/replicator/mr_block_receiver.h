//
// Created by peng on 2/17/23.
//

#pragma once

#include "peer/replicator/block_receiver.h"

#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "common/cv_wrapper.h"
#include "proto/block.h"

#include "bthread/countdown_event.h"
#include "bthread/butex.h"
#include "gtl/phmap.hpp"
#include <string>

namespace peer {

    class DefaultKeyStorage : public util::KeyStorage {
    public:
        bool saveKey(std::string_view ski, std::string_view raw, bool isPrivate, bool) override {
            keyMap[ski] = Cell{std::string(raw), isPrivate};
            return true;
        }

        auto loadKey(std::string_view ski) -> std::optional<std::pair<std::string, bool>> override {
            std::pair<std::string, bool> ret;
            bool contains = keyMap.if_contains(ski, [&ret](const KeyMap::value_type &v) {
                ret.first = v.second._raw;
                ret.second = v.second._isPrivate;
            });
            if (!contains) {
                return std::nullopt;
            }
            return ret;
        }

    private:
        struct Cell {
            // std::string _ski{}; key
            std::string _raw{}; // value
            bool _isPrivate{};
        };
        using KeyMap = gtl::parallel_flat_hash_map<std::string, Cell>;
        KeyMap keyMap;
    };

    class BSSCPWithThreadPool : public util::BCCSP {
    public:
        explicit BSSCPWithThreadPool() : BCCSP(std::make_unique<DefaultKeyStorage>()) { }

        // for validation only, no blocking options.
        mutable util::thread_pool_light tp;
    };

    class BFGWithThreadPool : public BlockFragmentGenerator {
    public:
        explicit BFGWithThreadPool(const std::vector<Config>& cfgList, std::unique_ptr<util::thread_pool_light> tp_)
                : BlockFragmentGenerator(cfgList, tp_.get()), tp(std::move(tp_)) { }

    private:
        std::unique_ptr<util::thread_pool_light> tp;
    };

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
            blockStorage[regionId].if_contains(blockId, [&block](
                    const RegionStorage::value_type &v) { block = v.second.block; });
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
        int waitForNewBlock(int regionId, int oldBlockNumber) {
            auto& futex = newBlockFutexList[regionId];
            return bthread::butex_wait(futex, oldBlockNumber, nullptr);
        }

        int onBlockPersist(int regionId, proto::BlockNumber blockNumber) {
            auto smallBlockNumber = (int) blockNumber;
            auto& futex = persistBlockFutexList[regionId];
            futex->store(smallBlockNumber);
            return bthread::butex_wake_all(futex);
        }

        // blockNumber is the maximum processed block
        // oldBlockNumber == -1 on starting
        int waitForBlockPersist(int regionId, int oldBlockNumber) {
            auto& futex = persistBlockFutexList[regionId];
            return bthread::butex_wait(futex, oldBlockNumber, nullptr);
        }

        int onReceivedNewBlock() {
            totalNewBlockCount->fetch_add(1, std::memory_order_relaxed);
            return bthread::butex_wake_all(totalNewBlockCount);
        }

        // currentBlockCount == 0 on starting
        int waitForNewBlock(int currentBlockCount) {
            return bthread::butex_wait(totalNewBlockCount, currentBlockCount, nullptr);
        }

        int onBlockPersist() {
            totalPersistBlockCount->fetch_add(1, std::memory_order_relaxed);
            return bthread::butex_wake_all(totalPersistBlockCount);
        }

        // currentBlockCount == 0 on starting
        int waitForBlockPersist(int currentBlockCount) {
            return bthread::butex_wait(totalPersistBlockCount, currentBlockCount, nullptr);
        }

    private:
        struct BlockCell {
            // block replicated successfully over n/2 regions
            bool persist = false;
            // the deserialized block and the related raw form
            std::shared_ptr<proto::Block> block = nullptr;
        };
        // key block id, value actual block
        using RegionStorage = gtl::parallel_flat_hash_map<proto::BlockNumber, BlockCell>;
        // multi region block storage
        std::vector<RegionStorage> blockStorage;
        // change when block updated
        butil::atomic<int>* totalNewBlockCount;
        butil::atomic<int>* totalPersistBlockCount;
        std::vector<butil::atomic<int>*> newBlockFutexList;
        std::vector<butil::atomic<int>*> persistBlockFutexList;
    };

    // MRBlockReceiver contains multiple BlockReceiver from different region,
    // and is responsible for things such as store and manage the entire blockchain.
    class MRBlockReceiver {
    public:
        MRBlockReceiver(const MRBlockReceiver&) = delete;

        MRBlockReceiver(MRBlockReceiver&&) = delete;

        // input serialized block, return deserialized block if validated
        // thread safe
        [[nodiscard]] std::shared_ptr<proto::Block> getBlockFromRawString(std::unique_ptr<std::string> raw) const {
            std::shared_ptr<proto::Block> block(new proto::Block);
            auto ret = block->deserializeFromString(std::move(raw));
            if (!ret.valid) {
                LOG(ERROR) << "Decode block failed!";
                return nullptr;
            }
            // validate block body signatures.
            auto signatureCnt = (int)block->metadata.consensusSignatures.size();
            bthread::CountdownEvent countdown(signatureCnt);
            std::atomic<int> verifiedSigCnt = 0;
            for (int i=0; i<signatureCnt; i++) {
                bccsp->tp.push_task([&, i=i] {
                    do {
                        auto& sig = block->metadata.consensusSignatures[i];
                        auto key = bccsp->GetKey(sig.ski);
                        if (key == nullptr) {
                            LOG(ERROR) << "Failed to found key, ski: " << sig.ski;
                            break;
                        }
                        // header + body serialized data
                        std::string_view serBody(block->getSerializedMessage()->data()+ret.headerPos, ret.execResultPos-ret.headerPos);
                        if (!key->Verify(sig.digest, serBody.data(), serBody.size())) {
                            LOG(ERROR) << "Sig validate failed, ski: " << sig.ski;
                            break;
                        }
                        verifiedSigCnt.fetch_add(1, std::memory_order_relaxed);
                    } while (false);
                    countdown.signal();
                });
            }
            countdown.wait();
            // TODO: thresh hold is enough
            if (verifiedSigCnt != signatureCnt) {
                LOG(ERROR) << "Signatures validate failed!";
                return nullptr;
            }
            // block is valid, return it.
            return block;
        }

        // start all the receiver
        bool checkAndStartService(proto::BlockNumber startAt) {
            auto regionCount = storage->regionCount();
            if (regionCount != regions.size()) {
                LOG(ERROR) << "Region size mismatch!";
                return false;
            }
            // set handle
            for (int i=0; i<(int)regionCount; i++) {
                auto validateFunc = [this, idx=i](std::string& raw, const std::vector<SingleRegionBlockReceiver::BufferBlock>& peerList) ->bool {
                    for (const auto& it: peerList) {
                        if (it.fragment->ebf.size != raw.size()) {
                            LOG(WARNING) << "Serialized block size mismatch!";
                            // TODO: BLOCK SIZE BYZANTINE ERROR HANDLING
                        }
                    }
                    auto block = getBlockFromRawString(std::make_unique<std::string>(std::move(raw)));
                    if (block == nullptr) {
                        LOG(ERROR) << "Can not generate block!";
                        return false;
                    }
                    // Check other stuff
                    if (peerList.empty()) {
                        LOG(ERROR) << "PeerList is empty!";
                        return false;
                    }
                    // check block number consist
                    auto blockNumber = block->header.number;
                    for (const auto& it: peerList) {
                        if (it.fragment->ebf.blockNumber != blockNumber) {
                            LOG(ERROR) << "Block number mismatch!";
                            return false;
                        }
                    }

                    storage->insertBlock(idx, std::move(block));
                    // wake up all consumer
                    storage->onReceivedNewBlock(idx, blockNumber);
                    storage->onReceivedNewBlock();
                    return true;
                };  // end of lambda
                // start service
                regions[i]->blockReceiver->activeStart(startAt, validateFunc);
            }
            return true;
        }

        std::shared_ptr<MRBlockStorage> getStorage() { return storage; }

        std::shared_ptr<BSSCPWithThreadPool> getBCCSP() { return bccsp; }

        std::shared_ptr<BFGWithThreadPool> getBFG() { return bfg; }

        // region count is bfgCfgList.size()
        static std::unique_ptr<MRBlockReceiver> NewMRBlockReceiver(
                // config of ALL nodes in ALL regions
                std::vector<SingleRegionBlockReceiver::ConfigPtr>& regionConfig,
                // erasure code sharding config for all regions
                const std::vector<BlockFragmentGenerator::Config>& bfgCfgList) {
            auto regionCount = bfgCfgList.size();
            // Create new instance
            std::unique_ptr<MRBlockReceiver> br(new MRBlockReceiver());
            br->bfg = std::make_shared<peer::BFGWithThreadPool>(bfgCfgList, std::make_unique<util::thread_pool_light>());
            // Init regions
            br->regions.reserve(regionCount);
            for (int i = 0; i < (int) regionCount; i++) {
                std::unique_ptr<RegionDS> rds(new RegionDS);
                // 1. process regionConfig
                for(auto& it: regionConfig) {
                    if (it == nullptr) {
                        continue;
                    }
                    // region id match
                    if (it->nodeConfig->groupId == i) {
                        rds->nodesConfig.push_back(std::move(it));
                        DCHECK(it == nullptr);  // prevent segment fault
                    }
                }
                if (rds->nodesConfig.empty()) {
                    LOG(ERROR) << "Nodes in a region is empty!";
                    return nullptr;
                }
                // 2. init blockReceiver
                rds->bfgConfig = bfgCfgList[i];
                rds->blockReceiver = SingleRegionBlockReceiver::NewSingleRegionBlockReceiver(br->bfg, rds->bfgConfig, rds->nodesConfig);
                if (rds->blockReceiver == nullptr) {
                    LOG(ERROR) << "Create SingleRegionBlockReceiver failed!";
                    return nullptr;
                }
                br->regions.push_back(std::move(rds));
            }
            // Init storage
            br->storage = std::make_shared<MRBlockStorage>(regionCount);
            // Init bccsp
            br->bccsp = std::make_shared<BSSCPWithThreadPool>();
            // Do not start the system
            return br;
        }

    protected:
        MRBlockReceiver() = default;

    private:
        // decode and encode block
        std::shared_ptr<BFGWithThreadPool> bfg;
        // receive block from multiple regions
        class RegionDS {
        public:
            peer::BlockFragmentGenerator::Config bfgConfig;
            std::vector<SingleRegionBlockReceiver::ConfigPtr> nodesConfig;
            std::unique_ptr<SingleRegionBlockReceiver> blockReceiver;
        };
        std::vector<std::unique_ptr<RegionDS>> regions;
        // store the whole blockchain
        std::shared_ptr<MRBlockStorage> storage;
        // for block signature validation
        std::shared_ptr<BSSCPWithThreadPool> bccsp;
    };

}
