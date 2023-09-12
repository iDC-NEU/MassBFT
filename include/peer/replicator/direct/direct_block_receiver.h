//
// Created by user on 23-9-9.
//

#pragma once

#include "peer/storage/mr_block_storage.h"
#include "common/concurrent_queue.h"
#include "common/zeromq.h"
#include "common/bccsp.h"
#include "common/property.h"
#include "proto/fragment.h"
#include <bthread/countdown_event.h>

namespace peer::direct {
    // BlockReceiver owns:
    // one RemoteFragmentReceiver (as server)
    // n-1 LocalFragmentReceiver (connect to local region server, except this one)
    // one FragmentRepeater (as local region server, broadcast remote fragments)
    class BlockReceiver {
    protected:
        // use an event loop to drain raw block from zmq sender.
        void runLocalReceiver(int idx) {
            pthread_setname_np(pthread_self(), "p2p_receiver");
            auto& instance = _receiverInstance[idx];
            while(!_tearDownSignal) {
                auto ret = instance->receive();
                if (ret == std::nullopt) {
                    break;  // socket dead
                }
                while (!_validateCallback) {    // todo: not thread safe
                    LOG(INFO) << "Wait for _validateCallback ready";
                }
                auto block = ret->to_string();
                _validateCallback(block);
            }
        }

        // use an event loop to drain block pieces from zmq sender.
        void runRemoteReceiver() {
            pthread_setname_np(pthread_self(), "remote_receiver");
            while(!_tearDownSignal) {
                auto ret = _remoteReceiverInstance->receive();
                if (ret == std::nullopt) {
                    break;  // socket dead
                }
                _localSenderInstance->send(*ret);
            }
        }

    public:
        using ConfigPtr = std::shared_ptr<util::ZMQInstanceConfig>;

        ~BlockReceiver() {
            _tearDownSignal = true;
            for (auto& it: _receiverInstance) {
                it->shutdown();
            }
            if (_remoteReceiverInstance) { _remoteReceiverInstance->shutdown(); }
            if (_localSenderInstance) { _localSenderInstance->shutdown(); }
            for (auto& it: _receiverThreads) {
                it->join();
            }
            if (_remoteReceiverThread) { _remoteReceiverThread->join(); }
        }

        // Each BlockReceiver instance listening on different rfrConfig and frConfig port
        static std::unique_ptr<BlockReceiver>  NewBlockReceiver(
                // one RemoteFragmentReceiver (as server)
                int rfrPort,
                // n-1 LocalFragmentReceiver (connect to local region server, except this one)
                // Local server can also be included, but localId needs to be set
                const std::vector<ConfigPtr>& lfrConfigList,
                // one FragmentRepeater (as local region server, broadcast remote fragments)
                int frPort) {
            if (lfrConfigList.empty() || std::min(rfrPort, frPort) <= 0 || rfrPort == frPort) {
                return nullptr;
            }
            // ensure the nodes are from the same group
            auto& n0 = lfrConfigList[0];
            for (int i=1; i<(int)lfrConfigList.size(); i++) {
                if (n0->nodeConfig->groupId != lfrConfigList[i]->nodeConfig->groupId) {
                    LOG(ERROR) << "GroupId is not the same!";
                    return nullptr;
                }
                if (n0->priAddr() == lfrConfigList[i]->priAddr() && n0->port == lfrConfigList[i]->port) {
                    LOG(ERROR) << "Two nodes in the same group have the same listen address!";
                    return nullptr;
                }
                if (n0->nodeConfig->ski == lfrConfigList[i]->nodeConfig->ski) {
                    LOG(ERROR) << "Two nodes have the same ski!";
                    return nullptr;
                }
            }
            std::unique_ptr<BlockReceiver> blockReceiver(new BlockReceiver());
            // set up _localFragmentReceiverList
            for (const auto& it: lfrConfigList) {
                auto localReceiver = util::ZMQInstance::NewClient<zmq::socket_type::sub>(it->priAddr(), it->port);
                if (!localReceiver) {
                    LOG(ERROR) << "Could not init localReceiver at port " << it->port;
                    return nullptr;
                }
                blockReceiver->_receiverInstance.push_back(std::move(localReceiver));
            }

            auto remoteReceiver = util::ZMQInstance::NewServer<zmq::socket_type::sub>(rfrPort);
            if (!remoteReceiver) {
                LOG(ERROR) << "Could not init remoteReceiver (zmq server) at port: " << rfrPort;
                return nullptr;
            }
            blockReceiver->_remoteReceiverInstance = std::move(remoteReceiver);
            auto localRepeater = util::ZMQInstance::NewServer<zmq::socket_type::pub>(frPort);
            if (!localRepeater) {
                LOG(ERROR) << "Could not init localRepeater (zmq server) at port: " << frPort;
                return nullptr;
            }
            blockReceiver->_localSenderInstance = std::move(localRepeater);
            return blockReceiver;
        }

        // if block deserialize failed, the owner of this class can BAN some nodes from nodesList
        using ValidateFunc = std::function<bool(std::string& rawBlock)>;

        void setValidateFunc(ValidateFunc func) { _validateCallback = std::move(func); }

        void activeStart() {
            for (int i=0; i<(int)_receiverInstance.size(); i++) {
                _receiverThreads.emplace_back(new std::thread(
                        &BlockReceiver::runLocalReceiver,
                        this,
                        i
                ));
            }
            _remoteReceiverThread = std::make_unique<std::thread>(
                    &BlockReceiver::runRemoteReceiver,
                    this
            );
        }

    protected:
        BlockReceiver() = default;

    private:
        std::vector<std::unique_ptr<std::thread>> _receiverThreads;
        std::vector<std::unique_ptr<util::ZMQInstance>> _receiverInstance;
        std::unique_ptr<std::thread> _remoteReceiverThread;
        std::unique_ptr<util::ZMQInstance> _remoteReceiverInstance;
        std::unique_ptr<util::ZMQInstance> _localSenderInstance;
        // Check the block signature and other things
        ValidateFunc _validateCallback;
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
    };

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
                std::unique_ptr<std::string> raw) const {
            std::shared_ptr<proto::Block> block(new proto::Block);
            auto ret = block->deserializeFromString(std::move(raw));
            if (!ret.valid) {
                LOG(ERROR) << "Decode block failed!";
                return nullptr;
            }
            CHECK(!block->metadata.consensusSignatures.empty()) << "Block signature is empty!";
            std::vector<proto::Block::SignaturePair>& signatures = block->metadata.consensusSignatures;
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
            // block is valid, return it.
            return block;
        }

    public:
        // start all the receiver
        bool checkAndStartService() {
            if (!storage || !bccsp || localRegionId==-1) {
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
                auto validateFunc = [this, idx=i](std::string& raw) ->bool {
                    auto block = getBlockFromRawString(std::make_unique<std::string>(std::move(raw)));
                    if (block == nullptr) {
                        LOG(ERROR) << "Can not generate block!";
                        return false;
                    }
                    std::unique_lock lock(storageMutex);    // prevent concurrent enter
                    if ((int)block->header.number <= storage->getMaxStoredBlockNumber(idx)) {
                        return true;    // already inserted
                    }
                    // LOG(INFO) << "Receive a block from remote, " << block->header.number;
                    storage->insertBlockAndNotify(idx, std::move(block));
                    return true;
                };  // end of lambda
                regions[i]->blockReceiver->setValidateFunc(std::move(validateFunc));
                regions[i]->blockReceiver->activeStart();
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
                rds->blockReceiver = BlockReceiver::NewBlockReceiver(rfrServerPorts.at(it.first),
                                                                     rds->nodesConfig,
                                                                     frServerPorts.at(it.first));
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

    protected:
        MRBlockReceiver() = default;

    private:
        int localRegionId = -1;
        // receive block from multiple regions
        class RegionDS {
        public:
            std::vector<BlockReceiver::ConfigPtr> nodesConfig;
            std::unique_ptr<BlockReceiver> blockReceiver;
        };
        std::unordered_map<int, std::unique_ptr<RegionDS>> regions;
        // store the whole blockchain
        std::shared_ptr<MRBlockStorage> storage;
        std::mutex storageMutex;
        // for block signature validation
        std::shared_ptr<util::BCCSP> bccsp;
        // thread pool for bccsp
        mutable std::shared_ptr<util::thread_pool_light> tp;
    };
}