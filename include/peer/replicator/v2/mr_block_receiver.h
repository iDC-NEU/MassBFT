//
// Created by peng on 2/17/23.
//

#pragma once

#include "peer/replicator/v2/block_receiver.h"
#include "peer/storage/mr_block_storage.h"

#include "common/thread_pool_light.h"
#include "common/bccsp.h"
#include "common/cv_wrapper.h"

#include "bthread/countdown_event.h"

namespace peer::v2 {
    // MRBlockReceiver contains multiple BlockReceiver from different region,
    // and is responsible for things such as store and manage the entire blockchain.
    class MRBlockReceiver {
    public:
        MRBlockReceiver(const MRBlockReceiver&) = delete;

        MRBlockReceiver(MRBlockReceiver&&) = delete;

    protected:
        // input serialized block, return deserialized block if validated
        // thread safe
        [[nodiscard]] std::shared_ptr<proto::Block> getBlockFromRawString(
                std::unique_ptr<std::string> raw,
                const std::vector<BlockReceiver::BufferBlock>& peerList) const {
            std::shared_ptr<proto::Block> block(new proto::Block);
            auto ret = block->deserializeFromString(std::move(raw));
            if (!ret.valid) {
                LOG(ERROR) << "Decode block failed!";
                return nullptr;
            }

            DCHECK(block->metadata.consensusSignatures.empty());
            // fill back consensus signatures
            // std::vector<proto::Block::SignaturePair> signatures;
            // {
            //     std::set<std::string_view> skiSet;
            //     for (const auto& it: peerList) {
            //         for (const auto& sig: it.fragment->ebf.blockSignatures) {
            //             if(!skiSet.insert(sig.second.ski).second) {
            //                 // item already exist
            //                 continue;
            //             }
            //             signatures.push_back(sig);
            //         }
            //     }
            // }
            // validate block body signatures.
            // TODO: aggregate peer signatures
            std::vector<proto::Block::SignaturePair> signatures = peerList[0].fragment->ebf.blockSignatures;
            auto signatureCnt = (int)signatures.size();
            bthread::CountdownEvent countdown(signatureCnt);
            std::atomic<int> verifiedSigCnt = 0;
            for (int i=0; i<signatureCnt; i++) {
                auto task = [&, i=i] {
                    do {
                        auto& sig = signatures[i].second;
                        auto key = bccsp->GetKey(sig.ski);
                        if (key == nullptr) {
                            LOG(ERROR) << "Failed to found key, ski: " << sig.ski;
                            break;
                        }
                        std::string_view serHeader(block->getSerializedMessage()->data(), ret.bodyPos);
                        if (!key->Verify(sig.digest, serHeader.data(), serHeader.size())) {
                            LOG(ERROR) << "Sig validate failed, ski: " << sig.ski;
                            break;
                        }
                        verifiedSigCnt.fetch_add(1, std::memory_order_relaxed);
                    } while (false);
                    countdown.signal();
                };
                util::PushEmergencyTask(tp.get(), task);
            }
            countdown.wait();
            // thresh hold is enough (f + 1)
            if (verifiedSigCnt < (signatureCnt + 1) / 2) {
                LOG(ERROR) << "Signatures validate failed!";
                return nullptr;
            }
            block->metadata.consensusSignatures = std::move(signatures);
            // block is valid, return it.
            return block;
        }

    public:
        // start all the receiver
        bool checkAndStartService(const std::unordered_map<int, proto::BlockNumber>& startAt) {
            if (!storage || !bccsp || !bfg || localRegionId==-1) {
                LOG(ERROR) << "Not init yet!";
                return false;
            }

            auto regionCount = (int)storage->regionCount();
            for (int i=0; i<regionCount; i++) {
                if (i == localRegionId) {
                    continue;
                }
                if (!regions.contains(i)) {
                    LOG(ERROR) << "Region size mismatch!";
                    return false;
                }
            }
            // set handle
            for (int i=0; i<(int)regionCount; i++) {
                if (i == localRegionId) {
                    continue;   // skip local region
                }
                auto validateFunc = [this, idx=i](std::string& raw, const std::vector<BlockReceiver::BufferBlock>& peerList) ->bool {
                    for (const auto& it: peerList) {
                        if (it.fragment->ebf.size != raw.size()) {
                            LOG(WARNING) << "Serialized block size mismatch!";
                            // TODO: BLOCK SIZE BYZANTINE ERROR HANDLING
                        }
                    }
                    auto block = getBlockFromRawString(std::make_unique<std::string>(std::move(raw)), peerList);
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

                    storage->insertBlockAndNotify(idx, std::move(block));
                    return true;
                };  // end of lambda
                regions[i]->blockReceiver->setValidateFunc(std::move(validateFunc));
                // start service
                proto::BlockNumber startBlockHeight = 0;
                if (startAt.contains(i)) {
                    startBlockHeight = startAt.at(i);
                }
                if (!regions[i]->blockReceiver->activeStart(startBlockHeight)) {
                    LOG(ERROR) << "start block receiver failed";
                    return false;
                }
            }
            return true;
        }

        // region count is bfgCfgList.size()
        static std::unique_ptr<MRBlockReceiver> NewMRBlockReceiver(
                // We do not receive fragments from local region
                const util::NodeConfigPtr& localNodeConfig,
                // frServerPorts are used to broadcast in the local zone chunk fragments received from the specified remote zone
                // In the case of multiple masters, there are multiple remote regions, so it is a map
                const std::unordered_map<int, int>& frServerPorts,  // regionId, port
                // rfrServerPorts are used to receive fragments from remote regions
                const std::unordered_map<int, int>& rfrServerPorts, // regionId, port
                // config of ALL local nodes of different multi-master instance (Except this node).
                // For the same node id, each [region] has a different port number
                // The port number is used to connect to other local servers
                // Each master region has a set of local ports
                const std::unordered_map<int, std::vector<BlockReceiver::ConfigPtr>>& regionConfig) {
            // Create new instance
            std::unique_ptr<MRBlockReceiver> br(new MRBlockReceiver());
            br->localRegionId = localNodeConfig->groupId;
            // Init regions
            auto localRegionId = localNodeConfig->groupId;
            for (const auto& it : regionConfig) {
                if (it.first == localRegionId) {
                    continue;   // skip local region
                }
                std::unique_ptr<RegionDS> rds(new RegionDS);
                // 1. process regionConfig
                for(const auto& cfg: it.second) {
                    // Connect to the local node for receiving relay fragments
                    if (cfg == nullptr || cfg->nodeConfig->groupId != localRegionId) {
                        LOG(WARNING) << "regionConfig may contains error!";
                        continue;
                    }
                    rds->nodesConfig.push_back(cfg);
                }
                if (rds->nodesConfig.empty()) {
                    LOG(ERROR) << "Nodes in a region is empty!";
                    return nullptr;
                }
                // 2. init blockReceiver
                rds->blockReceiver = BlockReceiver::NewBlockReceiver(localNodeConfig,
                                                                     rfrServerPorts.at(it.first),
                                                                     rds->nodesConfig,
                                                                     frServerPorts.at(it.first),
                                                                     localNodeConfig->nodeId);
                if (rds->blockReceiver == nullptr) {
                    LOG(ERROR) << "Create SingleRegionBlockReceiver failed!";
                    return nullptr;
                }
                br->regions[it.first] = std::move(rds);
            }
            // Do not start the system
            return br;
        }

        void setStorage(std::shared_ptr<MRBlockStorage> storage_) { storage = std::move(storage_); }

        std::shared_ptr<MRBlockStorage> getStorage() { return storage; }

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp_, std::shared_ptr<util::thread_pool_light> tp_=nullptr) {
            bccsp = std::move(bccsp_);
            tp = std::move(tp_);
        }

        std::shared_ptr<util::BCCSP> getBCCSP() { return bccsp; }

        bool setBFGWithConfig(
                // erasure code sharding instance for all regions
                const std::shared_ptr<BlockFragmentGenerator>& bfgInstance,
                // erasure code sharding config for all regions
                const std::unordered_map<int, BlockFragmentGenerator::Config>& bfgCfgList) {
            this->bfg = bfgInstance;
            for (auto& it: regions) {
                auto remoteGroupId = it.first;
                auto& rds = it.second;
                if (!bfgCfgList.contains(remoteGroupId)) {
                    LOG(ERROR) << "Can not find corresponding group id.";
                    return false;
                }
                rds->bfgConfig = bfgCfgList.at(remoteGroupId);
                rds->blockReceiver->setBFGConfig(bfgCfgList.at(remoteGroupId));
                rds->blockReceiver->setBFG(bfgInstance);
            }
            return true;
        }

        std::shared_ptr<BlockFragmentGenerator> getBFG() { return bfg; }

    protected:
        MRBlockReceiver() = default;

    private:
        int localRegionId = -1;
        // decode and encode block
        std::shared_ptr<BlockFragmentGenerator> bfg;
        // receive block from multiple regions
        class RegionDS {
        public:
            peer::BlockFragmentGenerator::Config bfgConfig;
            std::vector<BlockReceiver::ConfigPtr> nodesConfig;
            std::unique_ptr<BlockReceiver> blockReceiver;
        };
        std::unordered_map<int, std::unique_ptr<RegionDS>> regions;
        // store the whole blockchain
        std::shared_ptr<MRBlockStorage> storage;
        // for block signature validation
        std::shared_ptr<util::BCCSP> bccsp;
        // thread pool for bccsp
        mutable std::shared_ptr<util::thread_pool_light> tp;
    };
}
