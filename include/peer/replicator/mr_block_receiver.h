//
// Created by peng on 2/17/23.
//

#pragma once

#include "peer/replicator/block_receiver.h"
#include "peer/replicator/mr_block_storage.h"

#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "common/cv_wrapper.h"

#include "bthread/countdown_event.h"
#include "gtl/phmap.hpp"

namespace peer {

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
                auto task = [&, i=i] {
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
                };
                if (tp != nullptr) {
                    tp->push_task(task);
                } else {
                    task();
                }
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
            if (!storage || !bccsp || !bfg) {
                LOG(ERROR) << "Not init yet!";
                return false;
            }

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

        void setStorage(std::shared_ptr<MRBlockStorage> storage_) { storage = std::move(storage_); }

        std::shared_ptr<MRBlockStorage> getStorage() { return storage; }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp_, std::shared_ptr<util::thread_pool_light> tp_=nullptr) {
            bccsp = std::move(bccsp_);
            tp = std::move(tp_);
        }

        std::shared_ptr<util::BCCSP> getBCCSP() { return bccsp; }

        std::shared_ptr<BlockFragmentGenerator> getBFG() { return bfg; }

        // region count is bfgCfgList.size()
        static std::unique_ptr<MRBlockReceiver> NewMRBlockReceiver(
                // config of ALL nodes in ALL regions
                std::vector<SingleRegionBlockReceiver::ConfigPtr>& regionConfig,
                // erasure code sharding instance for all regions
                std::shared_ptr<BlockFragmentGenerator> bfg,
                // erasure code sharding config for all regions
                const std::vector<BlockFragmentGenerator::Config>& bfgCfgList) {
            // Create new instance
            std::unique_ptr<MRBlockReceiver> br(new MRBlockReceiver());
            br->bfg = std::move(bfg);
            // Init regions
            auto regionCount = bfgCfgList.size();
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
            // Do not start the system
            return br;
        }

    protected:
        MRBlockReceiver() = default;

    private:
        // decode and encode block
        std::shared_ptr<BlockFragmentGenerator> bfg;
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
        std::shared_ptr<util::BCCSP> bccsp;
        // thread pool for bccsp
        mutable std::shared_ptr<util::thread_pool_light> tp;
    };
}
