//
// Created by user on 23-3-23.
//

#pragma once

#include "common/pbft/pbft_state_machine.h"
#include "common/property.h"
#include "common/bccsp.h"
#include "common/thread_pool_light.h"
#include "common/concurrent_queue.h"

#include "proto/block.h"
#include "bthread/countdown_event.h"

namespace peer::consensus {
    class PBFTBlockCache;
}

namespace peer::consensus::v2 {
    class RequestReplicator;

    class LocalConsensus: public util::pbft::PBFTStateMachine {
    public:
        struct Config {
            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> targetNodes;
            int localId;
            int userRequestPort;
            // timeout for producing a block
            int timeoutMs;
            // batch size for producing a block
            int maxBatchSize;

            [[nodiscard]] std::shared_ptr<util::ZMQInstanceConfig> getNodeInfo(int nodeId) const {
                for (const auto& it: targetNodes) {
                    if (it->nodeConfig->nodeId == nodeId) {
                        return it;
                    }
                }
                return nullptr;
            }
        };
        // In order to separate the payload from the consensus,
        // the local cluster needs to establish a set of zmq ports, which are stored in targetNodes
        explicit LocalConsensus(Config config);

        ~LocalConsensus() override;

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _threadPoolForBCCSP = std::move(threadPool);
        }

        // This handle is called when the consensus of the block in local region is completed
        void setDeliverCallback(auto callback) { _deliverCallback = std::move(callback); }

        // Start the zeromq sender and receiver threads
        bool checkAndStart();

        // Notify rpc that the requests being executed return and unblock the system
        void sendStopSignal() { _running = false; }

        // If the user request has been verified before (pessimistic verification),
        // there is no need to re-verify here, otherwise the user signature needs to be verified
        bool pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>> batch);

    protected:
        [[nodiscard]] std::unique_ptr<::proto::Block::SignaturePair> OnSignProposal(const ::util::NodeConfigPtr& localNode, const std::string& message) override;

        // The following functions are called by state machine
        bool OnVerifyProposal(const ::util::NodeConfigPtr& localNode, const std::string& serializedHeader) override;

        bool OnDeliver(::util::NodeConfigPtr localNode, const std::string& context,
                       std::vector<::proto::Block::SignaturePair>&& signatures) override;

        void OnLeaderStart(::util::NodeConfigPtr localNode, int sequence) override;

        void OnLeaderChange(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence) override;

        // Call by the leader only
        std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr localNode, int sequence, const std::string& context) override;

    protected:
        class SignatureCache {
        public:
            std::unique_ptr<::proto::Block::SignaturePair> pop() {
                std::unique_lock lock(mutex);
                if (signature[front%10] == nullptr) {
                    return nullptr;
                }
                return std::move(signature[front++%10]);
            }

            void push(std::unique_ptr<::proto::Block::SignaturePair> sig) {
                std::unique_lock lock(mutex);
                signature[back++%10] = std::move(sig);
            }

            void reset() {
                std::unique_lock lock(mutex);
                for (auto & i : signature) {
                    i = nullptr;
                }
                front = 0;
                back = 0;
            }

        private:
            std::mutex mutex;
            int front = 0;
            int back = 0;
            std::unique_ptr<::proto::Block::SignaturePair> signature[10];
        };
        SignatureCache _signatureCache{};

    private:
        const Config _config;
        std::atomic<bool> _running;
        std::atomic<bool> _isLeader = false;
        // Used when the node become follower, receive txn batch from leader, using zeromq
        std::unique_ptr<PBFTBlockCache> _blockCache;
        std::unique_ptr<RequestReplicator> _requestReplicator;
        // Used when the node become leader, receive txn batch from user and enqueue to queue
        // TODO: consider limit the size of the queue
        util::BlockingConcurrentQueue<std::shared_ptr<::proto::Block>> _requestBatchQueue;
        // BCCSP and thread pool
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
        // For saving delivered blocks
        std::function<bool(std::shared_ptr<::proto::Block> block, ::util::NodeConfigPtr localNode)> _deliverCallback;
    };
}