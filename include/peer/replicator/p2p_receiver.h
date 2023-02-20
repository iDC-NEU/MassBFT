//
// Created by peng on 2/15/23.
//

#pragma once

#include "common/zeromq.h"
#include "proto/fragment.h"
#include "bthread/bthread.h"
#include "common/phmap.h"

namespace peer {
    // receive from a peer at another region
    // act as a zmq client
    // locate: int targetGroupId, int targetNodeId
    class P2PReceiver {
    protected:
        // use an event loop to drain block pieces from zmq sender.
        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "p2p_receiver");
            auto* instance=static_cast<P2PReceiver*>(ptr);
            while(true) {
                auto ret = instance->_clientSubscriber->receive();
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
        struct FragmentBlock {
            proto::EncodeBlockFragment ebf;
            zmq::message_t data;
        };
        using BlockNumber = proto::BlockNumber;

        virtual ~P2PReceiver() {
            // close the zmq instance to unblock local receiver thread.
            _clientSubscriber->shutdown();
            // join the event loop
            if (tid) { tid->join(); }
        }

        void start(std::unique_ptr<util::ZMQInstance> clientSubscriber) {
            _clientSubscriber = std::move(clientSubscriber);
            tid = std::make_unique<std::thread>(run, this);
        }

        void setOnMapUpdate(const auto& handle) { onMapUpdate = handle; }

        // The block number must increase only!
        bool addMessageToCache(zmq::message_t&& raw) {
            std::unique_ptr<FragmentBlock> fragmentBlock(new FragmentBlock{{}, std::move(raw)});
            std::string_view message(reinterpret_cast<const char*>(fragmentBlock->data.data()), fragmentBlock->data.size());
            // get the actual block fragment
            if(!fragmentBlock->ebf.deserializeFromString(message)) {
                LOG(ERROR) << "Decode message fragment failed!";
                return false;
            }
            auto blockNumber = fragmentBlock->ebf.blockNumber;
            if (blockNumber < nextReceiveBlockNumber) {
                LOG(ERROR) << "Stale block fragment, drop it, " << blockNumber;
                return false;
            } else if (blockNumber > nextReceiveBlockNumber+1) {
                LOG(WARNING) << "Block number leapfrog from: " << nextReceiveBlockNumber << " to: "<< blockNumber;
            }

            if (onMapUpdate) {
                onMapUpdate(blockNumber, std::move(fragmentBlock));
            } else {
                // store the fragment and the raw data
                map[blockNumber] = std::move(fragmentBlock);
            }
            nextReceiveBlockNumber = blockNumber;
            return true;
        }

        // if onMapUpdate is set, do not call this func, use the callback instead.
        std::unique_ptr<FragmentBlock> tryGet(BlockNumber blockNumber) {
            DCHECK(onMapUpdate == nullptr);
            std::unique_ptr<FragmentBlock> value = nullptr;
            map.erase_if(blockNumber, [&](auto& v) {
                if (v.second) {
                    value=std::move(v.second);
                    return true;
                }
                return false;
            });
            return value;
        }

    private:
        // the block number that consumer WILL get from map
        BlockNumber nextReceiveBlockNumber = 0;
        std::unique_ptr<std::thread> tid;
        std::unique_ptr<util::ZMQInstance> _clientSubscriber;
        util::MyFlatHashMap<BlockNumber, std::unique_ptr<FragmentBlock>, std::mutex> map;
        std::function<void(BlockNumber, std::unique_ptr<FragmentBlock>)> onMapUpdate;
    };
}