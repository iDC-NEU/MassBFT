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
            template<bool checkLow=true>
            void clear(proto::BlockNumber blockNumber) {
                if (checkLow) {
                    CHECK(low == blockNumber);
                }
                low = low+1;
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
            std::string addr;
            int port;
        };
        using ConfigPtr = std::shared_ptr<Config>;

        struct BufferBlock {
            std::unique_ptr<P2PReceiver::FragmentBlock> fragment;
            util::NodeConfigPtr nodeConfig;
        };

        // the owner of this
        SingleRegionBlockReceiver(
                std::shared_ptr<BlockFragmentGenerator> bfg
                , const BlockFragmentGenerator::Config& fragmentConfig
                , const std::vector<ConfigPtr>& nodesConfig)
                : _bfg(std::move(bfg))
                , _fragmentConfig(fragmentConfig) {
            for (const auto& it: nodesConfig) {
                auto receiver = std::make_unique<P2PReceiver>();
                receiver->setOnMapUpdate([cfg=it->nodeConfig, this](auto n, auto b){
                    _ringBuf.push(n, {std::move(b), cfg});
                });
                auto zmq = util::ZMQInstance::NewClient<zmq::socket_type::sub>(it->addr, it->port);
                if (!zmq) {
                    CHECK(false) << "Could not init SingleRegionBlockReceiver";
                }
                receiver->start(std::move(zmq));
            }
        }

        ~SingleRegionBlockReceiver() {
            _receiverList.clear();
            bthread_join(_tid, nullptr);
        }

        // active object version
        void activeStart(proto::BlockNumber startAt) {
            passiveStart(startAt);
            bthread_start_background(&_tid, &BTHREAD_ATTR_NORMAL, run, this);
        }

        // block may be nullptr;
        std::unique_ptr<std::string> activeGet() {
            std::unique_ptr<std::string> block;
            _activeBlockResultQueue.wait_dequeue(block);
            return block;
        }

        // passive object version
        void passiveStart(proto::BlockNumber startAt) {
            _ringBuf.clear<false>(startAt);
        }

        // block may be nullptr;
        std::unique_ptr<std::string> passiveGet(proto::BlockNumber number) {
            auto ret = genBlockFromQueue(number);
            if (!ret) {
                LOG(ERROR) << "can not regenerate block from fragments, block id: " << number;
            }
            _ringBuf.clear(number);
            return ret;
        }

    protected:
        static void* run(void* ptr) {
            auto* receiver = static_cast<SingleRegionBlockReceiver*>(ptr);
            auto& buf =  receiver->_ringBuf;
            auto nextBlockNumber = buf.nextBlock();
            while(true) {
                auto ret = receiver->passiveGet(nextBlockNumber);
                receiver->_activeBlockResultQueue.enqueue(std::move(ret));
                buf.clear(nextBlockNumber);
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
            BufferBlock tmp;
            const uint32_t minShardRequire = _fragmentConfig.dataShardCnt;
            const uint32_t totalShard = _fragmentConfig.dataShardCnt + _fragmentConfig.parityShardCnt;
            std::map<pmt::HashString, BlockRegenerateOptions> haveShard;    // TODO: make val.size byzantine free
            while(true) {
                queue.wait_dequeue(tmp);
                auto& ebf = tmp.fragment->ebf;
                if (ebf.blockNumber != number) {
                    LOG(WARNING) << "Receive incorrect block number: " << ebf.blockNumber << ", want:" << number;
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
                    auto msg = std::make_unique<std::string>();
                    if (!val.context->regenerateMessage((int)ebf.size, *msg)) {
                        LOG(WARNING) << "Regenerate shard failed.";
                        continue;
                    }
                    return msg;
                }
            }
        }

    private:
        bthread_t _tid{};
        std::shared_ptr<BlockFragmentGenerator> _bfg;
        const BlockFragmentGenerator::Config _fragmentConfig;
        Buffer<BufferBlock, 64> _ringBuf;
        moodycamel::BlockingConcurrentQueue<std::unique_ptr<std::string>> _activeBlockResultQueue;
        std::vector<std::unique_ptr<P2PReceiver>> _receiverList;
    };
}