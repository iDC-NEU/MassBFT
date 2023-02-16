//
// Created by peng on 2/15/23.
//

#pragma once

#include "common/zeromq.h"
#include "proto/block.h"
#include "bthread/bthread.h"
#include "gtl/phmap.hpp"
#include <string>

namespace peer {
    struct FragmentBlock {
        proto::EncodeBlockFragment ebf;
        zmq::message_t data;
    };
    using BlockNumber = proto::BlockNumber;

    // receive from a peer at another region
    // act as a zmq client
    // locate: int targetGroupId, int targetNodeId
    class P2PReceiver {
    protected:
        // use an event loop to drain block pieces from zmq sender.
        static void* run(void* ptr) {
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
        explicit P2PReceiver(std::unique_ptr<util::ZMQInstance> clientSubscriber)
        : _clientSubscriber(std::move(clientSubscriber)) {
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, run, this);
        }

        ~P2PReceiver() {
            // close the zmq instance to unblock local receiver thread.
            _clientSubscriber->shutdown();
            // join the event loop
            bthread_join(tid, nullptr);
        }

        void setOnMapUpdate(const std::function<void(BlockNumber)>& handle) {
            onMapUpdate = handle;
        }

        bool addMessageToCache(zmq::message_t&& raw) {
            std::unique_ptr<FragmentBlock> fragmentBlock(new FragmentBlock{{}, std::move(raw)});
            std::string_view message(reinterpret_cast<const char*>(fragmentBlock->data.data()), fragmentBlock->data.size());
            // get the actual block fragment
            zpp::bits::in inEBF(message);
            if(failure(inEBF(fragmentBlock->ebf))) {
                LOG(ERROR) << "Decode message fragment failed!";
                return false;
            }
            auto blockNumber = fragmentBlock->ebf.blockNumber;
            if (blockNumber<nextRequiredBlockNumber.load(std::memory_order_acquire)) {
                // TODO: Race condition, can not drop all stale requests!
                LOG(ERROR) << "Stale block fragment, drop it, " << blockNumber;
                return false;
            }
            // store the fragment and the raw data
            map[blockNumber] = std::move(fragmentBlock);
            if (onMapUpdate) {
                onMapUpdate(blockNumber);
            }
            return true;
        }

        std::unique_ptr<FragmentBlock> tryGet(BlockNumber blockNumber) {
            std::unique_ptr<FragmentBlock> value = nullptr;
            auto ret = map.erase_if(blockNumber, [&](auto& v) {
                if (v.second) {
                    value=std::move(v.second);
                    return true;
                }
                return false;
            });
            if (ret) {
                nextRequiredBlockNumber.store(blockNumber, std::memory_order_release);
                return value;
            }
            return nullptr;
        }

    private:
        // the block number that consumer WILL get from map
        std::atomic<BlockNumber> nextRequiredBlockNumber;
        bthread_t tid{};
        std::unique_ptr<util::ZMQInstance> _clientSubscriber;
        gtl::parallel_flat_hash_map<BlockNumber, std::unique_ptr<FragmentBlock>> map;
        std::function<void(BlockNumber)> onMapUpdate;
    };
}