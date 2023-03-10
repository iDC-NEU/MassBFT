//
// Created by user on 23-3-9.
//

#pragma once

#include "peer/replicator/mr_block_storage.h"
#include "peer/replicator/block_fragment_generator.h"
#include "peer/replicator/v2/fragment_util.h"

#include "common/reliable_zeromq.h"
#include "common/thread_pool_light.h"
#include "common/property.h"
#include "proto/block.h"
#include "proto/fragment.h"

#include "bthread/countdown_event.h"

namespace peer::v2 {
    // RemoteFragmentSender is responsible for packaging the fragments
    // and sending them to the specified server.
    class RemoteFragmentSender {
    public:
        ~RemoteFragmentSender() = default;

        RemoteFragmentSender(const RemoteFragmentSender &) = delete;

        RemoteFragmentSender(RemoteFragmentSender &&) = delete;

        // The caller init the bfg, call this function using a thread pool.
        // Multiple RemoteFragmentSender instance may run concurrently,
        // listening to different remote server address.
        bool encodeAndSendFragment(const BlockFragmentGenerator::Context &fragmentContext,
                                   proto::BlockNumber blockNumber,
                                   size_t blockSize) {
            DCHECK(checkContextValidity(fragmentContext.getConfig()));
            proto::EncodeBlockFragment localFragment;
            localFragment.blockNumber = blockNumber;
            localFragment.size = blockSize;
            localFragment.start = _start;
            localFragment.end = _end;
            localFragment.root = fragmentContext.getRoot();
            std::string localRawFragment;
            // serialize to string
            if (!localFragment.serializeToString(&localRawFragment, 0, false)) {
                LOG(ERROR) << "Serialize localFragment failed!";
                return false;
            }
            // append the actual encodeMessage to the back
            if (!fragmentContext.serializeFragments((int) localFragment.start,
                                                    (int) localFragment.end,
                                                    localRawFragment,
                                                    (int) localRawFragment.size())) {
                LOG(ERROR) << "Encode message fragment failed!";
                return false;
            }
            _sender->send(std::move(localRawFragment));
            return true;
        }

        static std::unique_ptr<RemoteFragmentSender> NewRFS(std::unique_ptr<util::ReliableZmqClient> sender, int start, int end) {
            if (start >= end || end <= 0 || sender==nullptr) {
                return nullptr;
            }
            std::unique_ptr<RemoteFragmentSender> rfs(new RemoteFragmentSender());
            rfs->_sender = std::move(sender);
            rfs->_start = start;
            rfs->_end = end;
            return rfs;
        }

    protected:
        RemoteFragmentSender() = default;

        [[nodiscard]] bool checkContextValidity(const BlockFragmentGenerator::Config &config) const {
            auto total = config.dataShardCnt + config.parityShardCnt;
            if (total <= _start || total < _end) {
                LOG(ERROR) << "RemoteFragmentSender input context error!";
                return false;
            }
            return true;
        }

    private:
        std::unique_ptr<util::ReliableZmqClient> _sender;
        // the start and end fragment id [start, end)
        int _start = 0;
        int _end = 0;
    };

    // BlockSender is responsible for sending locally generated fragments to a remote area (as a client).
    // One instance only connects to one remote area.
    // If you want to connect to multiple remote areas, you need to create multiple instances.
    class BlockSender {
    public:
        static std::unique_ptr<BlockSender>
        NewBlockSender(const FragmentUtil& fragmentConfig, int localId,
                       const std::function<std::shared_ptr<util::ZMQInstanceConfig>(int remoteId)>& getZMQConfigByRemoteId) {
            std::unique_ptr<BlockSender> blockSender(new BlockSender);
            auto fragmentCfgList = fragmentConfig.getSenderConfig(localId);
            for (const auto& it: fragmentCfgList) {
                auto zmqConfig = getZMQConfigByRemoteId(it.remoteId);
                if (zmqConfig == nullptr) {
                    LOG(ERROR) << "Can not load zmq config!";
                    return nullptr;
                }
                auto zmqClient = util::ReliableZmqClient::NewPublishClient(zmqConfig->addr(), zmqConfig->port);
                if (zmqClient == nullptr) {
                    LOG(ERROR) << "Can not create ReliableZmqClient!";
                    return nullptr;
                }
                std::unique_ptr<RemoteFragmentSender> rfs = RemoteFragmentSender::NewRFS(std::move(zmqClient), it.startFragmentId, it.endFragmentId);
                if (rfs == nullptr) {
                    LOG(ERROR) << "Can not init RemoteFragmentSender!";
                    return nullptr;
                }
                blockSender->_senders.push_back(std::move(rfs));
            }
            return blockSender;
        }

        // Encode the block and send it to the corresponding node in the remote AZ
        bool encodeAndSendBlock(const std::shared_ptr<proto::Block>& block) {
            auto context = _bfg->getEmptyContext(_remoteFragmentConfig);
            auto blockRaw = block->getSerializedMessage();
            if (blockRaw == nullptr) {
                // blockRaw is const string, add tmp variable to bypass modify it
                auto tmp = std::make_shared<std::string>();
                block->serializeToString(tmp.get());
                blockRaw = std::move(tmp);
            }
            context->initWithMessage(*blockRaw);
            // Using a thread pool is not necessary, since there are multiple regions process concurrently
            for(auto & _sender : _senders) {
                auto ret = _sender->encodeAndSendFragment(*context, block->header.number, blockRaw->size());
                if (!ret) {
                    LOG(ERROR) << "encodeAndSendFragment failed!";
                    return false;
                }
            }
            return true;
        }

        ~BlockSender() = default;

        BlockSender(const BlockSender&) = delete;

        BlockSender(BlockSender&&) = delete;

        // call by mr_block_sender
        void setBFG(std::shared_ptr<peer::BlockFragmentGenerator> bfg) { _bfg = std::move(bfg); }

        void setBFGConfig(const BlockFragmentGenerator::Config& remoteFragmentConfig) { _remoteFragmentConfig = remoteFragmentConfig; }

    protected:
        BlockSender() = default;

    private:
        // Remote bfg config
        peer::BlockFragmentGenerator::Config _remoteFragmentConfig;
        // Encode fragments, BlockSender do not own the sender instance
        // Since the same block will be sent to multiple AZs
        std::shared_ptr<peer::BlockFragmentGenerator> _bfg;
        // Send the fragments to multiple nodes (in the same remote region)
        std::vector<std::unique_ptr<RemoteFragmentSender>> _senders;
    };

    // MRBlockSender is responsible for sending blocks across domains to different "regions" (as a node)
    class MRBlockSender {
    public:
        MRBlockSender(const MRBlockSender&) = delete;

        MRBlockSender(MRBlockSender&&) = delete;

        using ConfigPtr = std::shared_ptr<util::ZMQInstanceConfig>;

        static std::unique_ptr<MRBlockSender> NewMRBlockSender(const std::vector<ConfigPtr>& allNodesList,
                                                               int localRegionId,
                                                               int localId,
                                                               std::shared_ptr<util::thread_pool_light> bfgWp = nullptr,
                                                               std::shared_ptr<util::thread_pool_light> blockSenderWp = nullptr) {
            std::unordered_map<int, std::vector<ConfigPtr>> allNodesMap;
            for (auto it: allNodesList) {
                allNodesMap[it->nodeConfig->groupId].push_back(std::move(it));
            }
            if (!allNodesMap.contains(localRegionId)) {
                LOG(INFO) << "allNodesList input error!";
                return nullptr;
            }
            std::unique_ptr<MRBlockSender> mrBlockSender(new MRBlockSender);
            mrBlockSender->_localRegionId = localRegionId;
            std::vector<BlockFragmentGenerator::Config> bfgCfgList;
            for (const auto& it: allNodesMap) {
                // skip local region
                if (it.first == localRegionId) {
                    continue;
                }
                // Generate the config of cluster to cluster broadcast.
                FragmentUtil fragmentUtil((int)allNodesMap[localRegionId].size(), (int)it.second.size());
                auto bfgCfg = fragmentUtil.getBFGConfig();
                auto senderConfig = fragmentUtil.getSenderConfig(localId);
                bfgCfgList.push_back(bfgCfg);
                auto getZMQConfigById = [&, remoteRegionId=it.first](int remoteId) ->std::shared_ptr<util::ZMQInstanceConfig> {
                    // find the node config and wrap it
                    for (const auto& nodeCfg: allNodesMap[remoteRegionId]) {
                        if (nodeCfg->nodeConfig->nodeId == remoteId) {
                            return nodeCfg;
                        }
                    }
                    return nullptr;
                };
                // init sender
                auto sender = BlockSender::NewBlockSender(fragmentUtil, localId, getZMQConfigById);
                if (sender == nullptr) {
                    return nullptr;
                }
                sender->setBFGConfig(bfgCfg);
                mrBlockSender->_blockSenderList.push_back(std::move(sender));
            }
            // create two thread pools
            if (blockSenderWp == nullptr) {
                blockSenderWp = std::make_shared<util::thread_pool_light>(std::min((int)allNodesMap.size()-1, (int)std::thread::hardware_concurrency()));
            }
            mrBlockSender->_wpForBlockSender = std::move(blockSenderWp);
            if (bfgWp == nullptr) {
                bfgWp = std::make_shared<util::thread_pool_light>();
            }
            mrBlockSender->_wpForBFG = std::move(bfgWp);
            // init bfg
            auto bfg = std::make_shared<BlockFragmentGenerator>(bfgCfgList, mrBlockSender->_wpForBFG.get());
            for (const auto& it: mrBlockSender->_blockSenderList) {
                it->setBFG(bfg);
            }

            return mrBlockSender;
        }

        virtual ~MRBlockSender() {
            _tearDownSignal = true;
            if (_thread) {
                _thread->join();
            }
        }

        void setStorage(std::shared_ptr<MRBlockStorage> storage) { _storage =std::move(storage); }

        bool checkAndStart(int startFromBlock) {
            if (!_wpForBlockSender || !_wpForBFG || !_storage) {
                LOG(ERROR) << "System have not init yet.";
                return false;
            }
            _thread = std::make_unique<std::thread>(&MRBlockSender::run, this, startFromBlock);
            return true;
        }

    protected:
        MRBlockSender() = default;

        void run(int startFromBlock) {
            pthread_setname_np(pthread_self(), "blk_sender");
            auto nextBlockNumber = startFromBlock;
            LOG(INFO) << "BlockSender start from block: " << nextBlockNumber;
            while(!_tearDownSignal) {
                auto timeout = butil::milliseconds_to_timespec(1000);
                if (!_storage->waitForNewBlock(_localRegionId, nextBlockNumber, &timeout)) {
                    continue;   // unexpected wakeup
                }
                auto block = _storage->getBlock(_localRegionId, nextBlockNumber);
                if (!block) {
                    LOG(INFO) << "Can not get block, retrying: " << nextBlockNumber;
                    continue;
                }
                bthread::CountdownEvent countdown((int)_blockSenderList.size());
                bool allSuccess = true;
                for (int i=0; i<(int)_blockSenderList.size(); i++) {
                    _wpForBlockSender->push_task([&, i=i](){
                        auto ret = _blockSenderList[i]->encodeAndSendBlock(block);
                        if (!ret) {
                            allSuccess = false;
                        }
                        countdown.signal();
                    });
                }
                countdown.wait();
                if (!allSuccess) {
                    LOG(ERROR) << "Can not send block, blk_sender quit: " << nextBlockNumber;
                    return;
                }
                nextBlockNumber++;
            }
        }

    private:
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
        std::unique_ptr<std::thread> _thread;
        int _localRegionId = -1;
        // MRBlockSender owns the bfg and the corresponding wp
        std::shared_ptr<util::thread_pool_light> _wpForBlockSender;
        std::shared_ptr<util::thread_pool_light> _wpForBFG;
        std::vector<std::unique_ptr<BlockSender>> _blockSenderList;
        // shared storage
        std::shared_ptr<MRBlockStorage> _storage;
    };
}