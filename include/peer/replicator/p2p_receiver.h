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
                auto ret = instance->subscriber->receive();
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
            subscriber->shutdown();
            // join the event loop
            if (tid) { tid->join(); }
        }

        [[nodiscard]] bool start(std::unique_ptr<util::ZMQInstance> subscriber_) {
            subscriber = std::move(subscriber_);
            if (onReceived == nullptr) {
                LOG(ERROR) << "onReceived handle not set!";
                return false;
            }
            tid = std::make_unique<std::thread>(run, this);
            return true;
        }

        void setOnReceived(const auto& handle) { onReceived = handle; }

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

            // the owner is responsible for validate fragment
            onReceived(blockNumber, std::move(fragmentBlock));
            nextReceiveBlockNumber = blockNumber;
            return true;
        }

    private:
        // the block number that consumer WILL receive
        BlockNumber nextReceiveBlockNumber = 0;
        std::unique_ptr<std::thread> tid;
        std::unique_ptr<util::ZMQInstance> subscriber;
        std::function<void(BlockNumber, std::unique_ptr<FragmentBlock>)> onReceived;
    };
}