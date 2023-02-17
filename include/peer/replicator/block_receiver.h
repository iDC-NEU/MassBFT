//
// Created by peng on 2/16/23.
//

#pragma once

#include "peer/replicator/p2p_receiver.h"
#include "peer/block_fragment_generator.h"

#include "common/property.h"
#include "blockingconcurrentqueue.h"
#include <array>
#include <vector>
#include <atomic>

#include "glog/logging.h"

namespace peer {
    // SingleRegionBlockReceiver owns multiple P2PReceivers from the same region
    class SingleRegionBlockReceiver {
    protected:
        constexpr static const auto DEQUEUE_TIMEOUT_US = 1000*1000;     // 1 sec

        template<class T, int cap, int mask=cap-1>
        class Buffer {
        public:
            // call by producer
            bool push(proto::BlockNumber blockNumber, T&& element) {
                if (blockNumber >= low+cap || blockNumber<low) {
                    return false;
                }
                data[blockNumber & mask].enqueue(std::move(element));
                return true;
            }

            // call by consumer
            // Need to check if the element in the queue have the same block number
            auto& get(proto::BlockNumber blockNumber) {
                if (blockNumber >= low+cap || blockNumber<low) {
                    CHECK(false) << "impl error!";
                }
                return data[blockNumber & mask];
            }

            // call by consumer
            // blocks below b are stale
            template<bool checkLow=true>
            void clearBelow(proto::BlockNumber b) {
                if (checkLow) {
                    CHECK(low+1 == b);
                }
                low = b;
            }

            // call by consumer
            [[nodiscard]] proto::BlockNumber nextBlock() const { return low; }

        private:
            // actual block number, the lowest data that are valid
            volatile proto::BlockNumber low = 0;
            std::array<moodycamel::BlockingConcurrentQueue<T>, cap> data;
        };

    public:
        struct Config {
            util::NodeConfigPtr nodeConfig;
            std::string& addr() {
                DCHECK(nodeConfig != nullptr) << "nodeConfig unset!";
                return nodeConfig->ip;
            }
            int port;
        };
        using ConfigPtr = std::shared_ptr<Config>;

        struct BufferBlock {
            std::unique_ptr<P2PReceiver::FragmentBlock> fragment;
            util::NodeConfigPtr nodeConfig;
        };

        ~SingleRegionBlockReceiver() {
            _tearDownSignal = true;
            _receiverList.clear();
            if (_tid) { _tid->join(); }
        }

        // active object version
        void activeStart(proto::BlockNumber startAt, const std::function<bool(std::string&)>& onBlockValidate=nullptr) {
            passiveStart(startAt, onBlockValidate);
            _tid = std::make_unique<std::thread>(run, this);
        }

        // block may be nullptr;
        std::unique_ptr<std::string> activeGet() {
            std::unique_ptr<std::string> block;
            // no need to be wait_dequeue_timed, because block may be nullptr
            _activeBlockResultQueue.wait_dequeue(block);
            return block;
        }

        // passive object version
        void passiveStart(proto::BlockNumber startAt, const std::function<bool(std::string&)>& onBlockValidate=nullptr) {
            _ringBuf.clearBelow<false>(startAt);
            _validateCallback = onBlockValidate;
        }

        // block may be nullptr;
        std::unique_ptr<std::string> passiveGet(proto::BlockNumber number) {
            auto ret = genBlockFromQueue(number);
            if (!ret) {
                LOG(ERROR) << "can not regenerate block from fragments, block id: " << number;
                return nullptr;
            }
            _ringBuf.clearBelow(number+1);
            return ret;
        }

        // the owner of this
        static std::unique_ptr<SingleRegionBlockReceiver> NewSingleRegionBlockReceiver(
                std::shared_ptr<BlockFragmentGenerator> bfg
                , const BlockFragmentGenerator::Config& fragmentConfig
                , const std::vector<ConfigPtr>& nodesConfig) {
            if (nodesConfig.empty() || !fragmentConfig.valid()) {
                return nullptr;
            }
            // ensure the nodes are from the same group
            auto& n0 = nodesConfig[0];
            for (int i=1; i<(int)nodesConfig.size(); i++) {
                if (n0->nodeConfig->groupId != nodesConfig[i]->nodeConfig->groupId) {
                    LOG(ERROR) << "GroupId is not the same!";
                    return nullptr;
                }
                if (n0->addr() == nodesConfig[i]->addr() && n0->port == nodesConfig[i]->port) {
                    LOG(ERROR) << "Two nodes have the same remote listen address!";
                    return nullptr;
                }
                if (n0->nodeConfig->ski == nodesConfig[i]->nodeConfig->ski) {
                    LOG(ERROR) << "Two nodes have the same ski!";
                    return nullptr;
                }
            }
            std::unique_ptr<SingleRegionBlockReceiver> srBlockReceiver(new SingleRegionBlockReceiver(std::move(bfg), fragmentConfig));
            // spin up clients
            for (const auto& it: nodesConfig) {
                auto receiver = std::make_unique<P2PReceiver>();
                receiver->setOnMapUpdate([cfg=it->nodeConfig, ptr=srBlockReceiver.get()](auto n, auto b){
                    ptr->_ringBuf.push(n, {std::move(b), cfg});
                });
                auto zmq = util::ZMQInstance::NewClient<zmq::socket_type::sub>(it->addr(), it->port);
                if (!zmq) {
                    LOG(ERROR) << "Could not init SingleRegionBlockReceiver";
                    return nullptr;
                }
                receiver->start(std::move(zmq));
                srBlockReceiver->_receiverList.push_back(std::move(receiver));
            }
            return srBlockReceiver;
        }

    protected:
        // the owner of this
        SingleRegionBlockReceiver(
                std::shared_ptr<BlockFragmentGenerator> bfg
                , const BlockFragmentGenerator::Config& fragmentConfig)
                : _bfg(std::move(bfg))
                , _fragmentConfig(fragmentConfig) { }

        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "blk_receiver");
            auto* receiver = static_cast<SingleRegionBlockReceiver*>(ptr);
            auto& buf =  receiver->_ringBuf;
            auto nextBlockNumber = buf.nextBlock();
            LOG(INFO) << "SingleRegionBlockReceiver active get start from block: " << nextBlockNumber;
            while(!receiver->_tearDownSignal) {
                auto ret = receiver->passiveGet(nextBlockNumber);
                if (ret == nullptr) {
                    continue;
                }
                receiver->_activeBlockResultQueue.enqueue(std::move(ret));
                nextBlockNumber++;
            }
            return nullptr;
        }

        // access by only a consumer
        std::unique_ptr<std::string> genBlockFromQueue(proto::BlockNumber number) {

            struct BlockRegenerateOptions {
                uint32_t currentShardCnt = 0;
                std::vector<bool> shardsValidated;
                std::shared_ptr<BlockFragmentGenerator::Context> context;
            };

            auto& queue = _ringBuf.get(number);
            // There may exist multiple fragments, so multiple slot is needed.
            std::vector<BufferBlock> blockList;
            const uint32_t minShardRequire = _fragmentConfig.dataShardCnt;
            const uint32_t totalShard = _fragmentConfig.dataShardCnt + _fragmentConfig.parityShardCnt;
            std::map<pmt::HashString, BlockRegenerateOptions> haveShard;    // TODO: make val.size byzantine free
            while (!_tearDownSignal) {
                {
                    BufferBlock tmp;
                    if (!queue.wait_dequeue_timed(tmp, DEQUEUE_TIMEOUT_US)) {
                        continue;   // retry after timeout
                    }
                    blockList.push_back(std::move(tmp));
                }
                auto& ebf = blockList.back().fragment->ebf;
                if (ebf.blockNumber != number) {
                    if (ebf.blockNumber > number) {
                        LOG(WARNING) << "Receive incorrect block number: " << ebf.blockNumber << ", want:" << number;
                    }
                    continue;
                }
                auto& val = haveShard[ebf.root];
                if (val.currentShardCnt == 0) {
                    val.shardsValidated.resize(totalShard);
                    val.context = _bfg->getEmptyContext(_fragmentConfig);
                }
                if (totalShard<ebf.start+1 || totalShard<ebf.end || ebf.start>=ebf.end) {
                    LOG(WARNING) << "Shard type inconsistent.";
                    continue;
                }
                if (!val.context->validateAndDeserializeFragments(ebf.root, ebf.encodeMessage, (int)ebf.start, (int)ebf.end)) {
                    LOG(WARNING) << "Shard corrupted.";
                    continue;
                }
                for (uint32_t i=ebf.start; i<ebf.end; i++) {
                    if (!val.shardsValidated[i]) {
                        val.shardsValidated[i] = true;
                        val.currentShardCnt++;
                    }
                }
                if (val.currentShardCnt>=minShardRequire) {
                    // 1. verify the block integrity
                    auto msg = std::make_unique<std::string>();
                    if (!val.context->regenerateMessage((int)ebf.size, *msg)) {
                        LOG(WARNING) << "Regenerate shard failed.";
                        continue;
                    }
                    // 2. validate the block integrity (if needed)
                    if (_validateCallback != nullptr) {
                        // validate failed, received block is generated by byzantine nodes
                        if (!_validateCallback(*msg)) {
                            continue;
                        }
                    }
                    // In phase 2, the user can take the content, making the return value empty.
                    return msg;
                }
            }
            return nullptr;
        }

    private:
        // For active object, tid and message queue
        std::unique_ptr<std::thread> _tid;
        moodycamel::BlockingConcurrentQueue<std::unique_ptr<std::string>> _activeBlockResultQueue;
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
        // bfg and the remote region fragment config
        std::shared_ptr<BlockFragmentGenerator> _bfg;
        const BlockFragmentGenerator::Config _fragmentConfig;
        Buffer<BufferBlock, 64> _ringBuf;
        // Check the block signature and other things
        std::function<bool(std::string&)> _validateCallback = nullptr;
        // a list of fragment receiver
        std::vector<std::unique_ptr<P2PReceiver>> _receiverList;
    };
}