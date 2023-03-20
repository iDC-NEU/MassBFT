//
// Created by user on 23-3-11.
//

#pragma once

#include "peer/replicator/block_fragment_generator.h"
#include "common/reliable_zeromq.h"
#include "common/property.h"
#include "proto/fragment.h"

#include "blockingconcurrentqueue.h"
#include <memory>
#include <thread>
#include <utility>

namespace peer::v2 {
    struct FragmentBlock {
        proto::EncodeBlockFragment ebf;
        zmq::message_t data;
    };

    template<class ZMQInstanceType>
    requires requires (ZMQInstanceType i) { i.shutdown(); i.receive(); }
    class FragmentReceiver {
    protected:
        // use an event loop to drain block pieces from zmq sender.
        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "p2p_receiver");
            auto* instance=static_cast<FragmentReceiver*>(ptr);
            while(true) {
                auto ret = instance->_sub->receive();
                if (ret == std::nullopt) {
                    LOG(ERROR) << "Receive message fragment failed!";
                    break;  // socket dead
                }
                // analyse the fragment and push into storage
                if (!instance->addMessageToCache(std::move(*ret))) {
                    LOG(ERROR) << "Failed to deserialize fragment!";
                }
            }
            return nullptr;
        }

    public:
        using BlockNumber = proto::BlockNumber;

        virtual ~FragmentReceiver() {
            // close the zmq instance to unblock local receiver thread.
            if (_sub) { _sub->shutdown(); }
            // join the event loop
            if (_thread) { _thread->join(); }
        }

        FragmentReceiver() = default;

        FragmentReceiver(FragmentReceiver&&) = delete;

        FragmentReceiver(const FragmentReceiver&) = delete;

        [[nodiscard]] bool checkAndStart(std::shared_ptr<ZMQInstanceType> sub) {
            _sub = std::move(sub);
            if (onReceived == nullptr) {
                LOG(ERROR) << "onReceived handle not set!";
                return false;
            }
            _thread = std::make_unique<std::thread>(run, this);
            return true;
        }

        void setOnReceived(const auto& handle) { onReceived = handle; }

        bool addMessageToCache(zmq::message_t&& raw) {
            std::unique_ptr<FragmentBlock> fragmentBlock(new FragmentBlock{{}, std::move(raw)});
            std::string_view message(reinterpret_cast<const char*>(fragmentBlock->data.data()), fragmentBlock->data.size());
            // get the actual block fragment
            if(!fragmentBlock->ebf.deserializeFromString(message)) {
                LOG(ERROR) << "Decode message fragment failed!";
                return false;
            }
            // The block numbers corresponding to the received fragments may be out of order,
            // we do not purge the received fragments here
            // the owner is responsible for validate fragment
            auto blockNumber = fragmentBlock->ebf.blockNumber;
            onReceived(blockNumber, std::move(fragmentBlock));
            return true;
        }

    private:
        std::unique_ptr<std::thread> _thread;
        std::shared_ptr<ZMQInstanceType> _sub;
        std::function<void(BlockNumber, std::unique_ptr<FragmentBlock>)> onReceived;
    };

    // LocalFragmentReceiver act as zmq server, receive fragments from mr_block_sender
    using RemoteFragmentReceiver = FragmentReceiver<util::ReliableZmqServer>;

    // LocalFragmentReceiver act as zmq client
    using LocalFragmentReceiver = FragmentReceiver<util::ZMQInstance>;

    // FragmentRepeater act as zmq server
    using FragmentRepeater = util::ZMQInstance;

    // BlockReceiver owns:
    // one RemoteFragmentReceiver (as server)
    // n-1 LocalFragmentReceiver (connect to local region server, except this one)
    // one FragmentRepeater (as local region server, broadcast remote fragments)
    class BlockReceiver {
    protected:
        constexpr static const auto DEQUEUE_TIMEOUT_US = 1000*100;     // 100 ms

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
                    CHECK(low+1 == b) << "impl error!";
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
        using ConfigPtr = std::shared_ptr<util::ZMQInstanceConfig>;

        struct BufferBlock {
            std::unique_ptr<FragmentBlock> fragment;
            util::NodeConfigPtr nodeConfig;
        };

        // if block deserialize failed, the owner of this class can BAN some nodes from nodesList
        using ValidateFunc = std::function<bool(std::string& rawBlock, const std::vector<BufferBlock>& nodesList)>;

        ~BlockReceiver() {
            _tearDownSignal = true;
            _remoteFragmentReceiver.reset();
            _localFragmentReceiverList.clear();
            if (_thread) { _thread->join(); }
        }

        // Each BlockReceiver instance listening on different rfrConfig and frConfig port
        static std::unique_ptr<BlockReceiver> NewBlockReceiver(
                util::NodeConfigPtr localNodeConfig,
                // one RemoteFragmentReceiver (as server)
                int rfrPort,
                // n-1 LocalFragmentReceiver (connect to local region server, except this one)
                // Local server can also be included, but localId needs to be set
                const std::vector<ConfigPtr>& lfrConfigList,
                // one FragmentRepeater (as local region server, broadcast remote fragments)
                int frPort,
                int localId) {
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
                if (n0->addr() == lfrConfigList[i]->addr() && n0->port == lfrConfigList[i]->port) {
                    LOG(ERROR) << "Two nodes have the same remote listen address!";
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
                if (it->nodeConfig->nodeId == localId) {
                    continue;   // skip local id
                }
                auto receiver = std::make_unique<LocalFragmentReceiver>();
                receiver->setOnReceived([cfg = it->nodeConfig, ptr = blockReceiver.get()](auto n, auto b) {
                    // DLOG(INFO) << "Receive a block from local broadcast, block number: " << n;
                    if (!ptr->_ringBuf.push(n, {std::move(b), cfg})) {
                        LOG(ERROR) << "Can not enqueue to ring buffer, block fragment may be lost!";
                    }
                });
                auto zmq = util::ZMQInstance::NewClient<zmq::socket_type::sub>(it->addr(), it->port);
                if (!zmq) {
                    LOG(ERROR) << "Could not init localFragmentReceiver!";
                    return nullptr;
                }
                if (!receiver->checkAndStart(std::move(zmq))) {
                    return nullptr;
                }
                blockReceiver->_localFragmentReceiverList.push_back(std::move(receiver));
            }
            // set up _remoteFragmentReceiver
            auto rfr = std::make_unique<RemoteFragmentReceiver>();
            rfr->setOnReceived([cfg = std::move(localNodeConfig), ptr = blockReceiver.get()](auto n, auto b) {
                // repeat the block
                // TODO: OMIT ADDITIONAL COPY
                ptr->_fragmentRepeater->send(b->data.to_string());
                // add to ring buffer
                // DLOG(INFO) << "Receive a block from remote broadcast, block number: " << n;
                if (!ptr->_ringBuf.push(n, {std::move(b), cfg})) {
                    LOG(ERROR) << "Can not enqueue to ring buffer, block fragment may be lost!";
                }
            });
            auto ret = util::ReliableZmqServer::NewSubscribeServer(rfrPort);
            if (!ret) {
                LOG(ERROR) << "Could not init remoteFragmentReceiver!";
                return nullptr;
            }
            if (!rfr->checkAndStart(util::ReliableZmqServer::GetSubscribeServer(rfrPort))) {
                return nullptr;
            }
            blockReceiver->_remoteFragmentReceiver = std::move(rfr);
            // set up _fragmentRepeater
            blockReceiver->_fragmentRepeater = FragmentRepeater::NewServer<zmq::socket_type::pub>(frPort);
            if (!blockReceiver->_fragmentRepeater) {
                LOG(ERROR) << "Could not init fragmentRepeater!";
                return nullptr;
            }
            return blockReceiver;
        }

        // call by mr_block_receiver
        void setBFG(std::shared_ptr<peer::BlockFragmentGenerator> bfg) { _bfg = std::move(bfg); }

        void setBFGConfig(const BlockFragmentGenerator::Config& cfg) { _localFragmentConfig = cfg; }

        void setValidateFunc(ValidateFunc func) { _validateCallback = std::move(func); }

        // passive object version
        bool passiveStart(proto::BlockNumber startAt) {
            if (!_fragmentRepeater || !_remoteFragmentReceiver || !_bfg) {
                return false;
            }
            _ringBuf.clearBelow<false>(startAt);
            return true;
        }

        // active object version
        bool activeStart(proto::BlockNumber startAt) {
            if (!passiveStart(startAt)) {
                return false;
            }
            _thread = std::make_unique<std::thread>(run, this);
            return true;
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

        // block may be nullptr;
        std::unique_ptr<std::string> activeGet() {
            std::unique_ptr<std::string> block;
            // no need to be wait_dequeue_timed, because block may be nullptr
            _activeBlockResultQueue.wait_dequeue(block);
            return block;
        }

    protected:
        BlockReceiver() = default;

        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "blk_receiver");
            auto* receiver = static_cast<BlockReceiver*>(ptr);
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
            std::map<pmt::HashString, std::vector<BufferBlock>> blockMap;
            const uint32_t minShardRequire = _localFragmentConfig.dataShardCnt;
            const uint32_t totalShard = _localFragmentConfig.dataShardCnt + _localFragmentConfig.parityShardCnt;
            std::map<pmt::HashString, BlockRegenerateOptions> haveShard;    // TODO: make val.size byzantine free
            while (!_tearDownSignal) {
                BufferBlock tmp;
                if (!queue.wait_dequeue_timed(tmp, DEQUEUE_TIMEOUT_US)) {
                    continue;   // retry after timeout
                }
                auto& ebf = tmp.fragment->ebf;
                DLOG(INFO) << "Process Block number " << ebf.blockNumber << " with fragment start with " << ebf.start;
                if (ebf.blockNumber != number) {
                    if (ebf.blockNumber > number) {
                        LOG(WARNING) << "Receive incorrect block number: " << ebf.blockNumber << ", want:" << number;
                    }
                    continue;
                }
                auto& val = haveShard[ebf.root];
                if (val.currentShardCnt == 0) {
                    val.shardsValidated.resize(totalShard);
                    val.context = _bfg->getEmptyContext(_localFragmentConfig);
                }
                if (totalShard<ebf.start+1 || totalShard<ebf.end || ebf.start>=ebf.end) {
                    LOG(WARNING) << "Shard type inconsistent.";
                    continue;
                }
                if (!val.context->validateAndDeserializeFragments(ebf.root, ebf.encodeMessage, (int)ebf.start, (int)ebf.end)) {
                    LOG(WARNING) << "Shard corrupted.";
                    continue;
                }
                // shard valid (for now), push into buffer to store it
                blockMap[ebf.root].push_back(std::move(tmp));
                for (uint32_t i=ebf.start; i<ebf.end; i++) {
                    if (!val.shardsValidated[i]) {
                        val.shardsValidated[i] = true;
                        val.currentShardCnt++;
                    }
                }
                // Check if we have enough shards to regenerate data
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
                        if (!_validateCallback(*msg, blockMap[ebf.root])) {
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
        std::unique_ptr<std::thread> _thread;
        moodycamel::BlockingConcurrentQueue<std::unique_ptr<std::string>> _activeBlockResultQueue;
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
        // bfg and the remote region fragment config
        BlockFragmentGenerator::Config _localFragmentConfig;
        std::shared_ptr<BlockFragmentGenerator> _bfg;
        // Cache the received fragments
        Buffer<BufferBlock, 64> _ringBuf;
        // Check the block signature and other things
        ValidateFunc _validateCallback;
        // one RemoteFragmentReceiver (as server)
        std::unique_ptr<RemoteFragmentReceiver> _remoteFragmentReceiver;
        // n-1 LocalFragmentReceiver (connect to local region server, except this one)
        std::vector<std::unique_ptr<LocalFragmentReceiver>> _localFragmentReceiverList;
        // one FragmentRepeater (as local region server, broadcast remote fragments)
        std::unique_ptr<FragmentRepeater> _fragmentRepeater;
    };
}