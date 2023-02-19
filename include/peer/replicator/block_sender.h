//
// Created by peng on 2/18/23.
//

#pragma once

#include "peer/replicator/mr_block_storage.h"
#include "peer/block_fragment_generator.h"
#include "common/zeromq.h"
#include "common/property.h"
#include "butil/time.h"
#include "proto/fragment.h"

namespace peer {
    class BlockSender {
    public:
        using Config = util::ZMQInstanceConfig;
        using ConfigPtr = std::shared_ptr<Config>;

        BlockSender() = default;

        BlockSender(const BlockSender&) = delete;

        BlockSender(BlockSender&&) = delete;

        virtual ~BlockSender() {
            _tearDownSignal = true;
            if (_tid) {
                _tid->join();
            }
        }

        void setStorage(std::shared_ptr<peer::MRBlockStorage> storage) {
            _storage = std::move(storage);
        }

        void setBFG(const peer::BlockFragmentGenerator::Config& localBfgConfig, std::shared_ptr<peer::BlockFragmentGenerator> bfg) {
            _localBfgConfig = localBfgConfig;
            _bfg = std::move(bfg);
        }

        // localServerConfig: Local zmq configuration
        // regionServerCount: Total number of nodes in the region
        void setLocalServerConfig(ConfigPtr localServerConfig, int regionServerCount) {
            _localServerConfig = std::move(localServerConfig);
            _regionServerCount = regionServerCount;
        }

        bool checkAndStart(int startFromBlock) {
            auto localRegionId = _localServerConfig->nodeConfig->groupId;
            if (_storage == nullptr || (int)_storage->regionCount() <= localRegionId) {
                LOG(ERROR) << "Can not start block sender, because config wrong.";
                return false;
            }
            _localServer = util::ZMQInstance::NewServer<zmq::socket_type::pub>(_localServerConfig->port);
            if (!_localServer || !_bfg || _regionServerCount <= 0) {
                LOG(ERROR) << "System have not init yet.";
                return false;
            }
            calculateLocalFragmentId();
            _tid = std::make_unique<std::thread>(&BlockSender::run, this, startFromBlock);
            return true;
        }

    protected:
        void calculateLocalFragmentId() {
            int totalServers = _regionServerCount;
            int totalFragments = _localBfgConfig.dataShardCnt+_localBfgConfig.parityShardCnt;
            int localId = _localServerConfig->nodeConfig->nodeId;
            auto fragmentSPerServer = totalFragments/totalServers;
            start = fragmentSPerServer*localId;
            end = fragmentSPerServer*(localId+1);
        }

        void run(int startFromBlock) {
            pthread_setname_np(pthread_self(), "blk_sender");
            auto nextBlockNumber = startFromBlock;
            auto regionId = _localServerConfig->nodeConfig->groupId;
            LOG(INFO) << "BlockSender start from block: " << nextBlockNumber;
            while(!_tearDownSignal) {
                auto timeout = butil::milliseconds_to_timespec(1000);
                if (!_storage->waitForNewBlock(regionId, nextBlockNumber, &timeout)) {
                    continue;   // unexpected wakeup
                }
                auto block = _storage->getBlock(regionId, nextBlockNumber);
                if (!block) {
                    LOG(INFO) << "Can not get block, retrying: " << nextBlockNumber;
                    continue;
                }
                DLOG(INFO) << "Region " << regionId <<" get block from storage: " << nextBlockNumber;
                if (!encodeAndSendBlock(block)) {
                    CHECK(false) << "Can not send block: " << nextBlockNumber;
                }
                nextBlockNumber++;
            }
        }

        // synchronous call
        // Must be thread safe, encode and send a block
        // caller set most of the fields in localFragment
        bool encodeAndSendBlock(const std::shared_ptr<proto::Block>& block) {
            proto::EncodeBlockFragment localFragment;
            localFragment.blockNumber = block->header.number;
            auto context = _bfg->getEmptyContext(_localBfgConfig);
            auto blockRaw = block->getSerializedMessage();
            if (blockRaw == nullptr) {
                auto tmp = std::make_shared<std::string>();
                block->serializeToString(tmp.get());
                blockRaw = std::move(tmp);
            }
            context->initWithMessage(*blockRaw);
            localFragment.size = blockRaw->size();
            localFragment.start = start;
            localFragment.end = end;
            localFragment.root = context->getRoot();
            std::string localRawFragment;
            // serialize to string
            if(!localFragment.serializeToString(&localRawFragment, 0, false)) {
                LOG(ERROR) << "Serialize localFragment failed!";
                return false;
            }
            if(!context->serializeFragments((int)localFragment.start,
                                            (int)localFragment.end,
                                            localRawFragment,
                                            (int)localRawFragment.size())) {
                LOG(ERROR) << "Encode message fragment failed!";
                return false;
            }

            _localServer->send(std::move(localRawFragment));
            return true;
        }

    private:
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
        std::unique_ptr<std::thread> _tid;
        int _regionServerCount = 0;
        std::shared_ptr<peer::MRBlockStorage> _storage;
        // to generate fragments
        peer::BlockFragmentGenerator::Config _localBfgConfig;
        std::shared_ptr<peer::BlockFragmentGenerator> _bfg;
        // the start and end fragment id [start, end)
        int start = 0;
        int end = 0;
        // to broadcast (corresponding) fragments
        ConfigPtr _localServerConfig;
        std::unique_ptr<util::ZMQInstance> _localServer;
    };
}