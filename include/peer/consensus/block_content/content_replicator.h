//
// Created by user on 23-3-23.
//

#pragma once

#include "common/pbft/pbft_state_machine.h"
#include "common/zeromq.h"
#include "common/property.h"
#include "common/bccsp.h"
#include "common/phmap.h"
#include "common/thread_pool_light.h"
#include "common/concurrent_queue.h"

#include "proto/block.h"
#include "bthread/countdown_event.h"

namespace peer::consensus {
    class ContentSender;

    class ContentReceiver;

    class PBFTBlockCache;

    class ContentReplicator: public util::pbft::PBFTStateMachine {
    public:
        // In order to separate the payload from the consensus,
        // the local cluster needs to establish a set of zmq ports, which are stored in targetNodes
        ContentReplicator(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& targetNodes, int localId);

        ~ContentReplicator() override;

        void setBCCSPWithThreadPool(std::shared_ptr<util::BCCSP> bccsp, std::shared_ptr<util::thread_pool_light> threadPool) {
            _bccsp = std::move(bccsp);
            _threadPoolForBCCSP = std::move(threadPool);
        }

        [[nodiscard]] auto getThreadPoolForBCCSP() const { return _threadPoolForBCCSP; }

        // This handle is called when the consensus of the block in local region is completed
        void setDeliverCallback(auto callback) { _deliverCallback = std::move(callback); }

        // This function is called when the master node changes, thread safe
        // leader may become itself, newLeaderNode is empty when the master node is itself
        void setLeaderChangeCallback(auto callback) { _leaderChangeCallback = std::move(callback); }

        // Start the zeromq sender and receiver threads
        bool checkAndStart();

        // Notify rpc that the requests being executed return and unblock the system
        void sendStopSignal() { _running = false; }

        // If the user request has been verified before (pessimistic verification),
        // there is no need to re-verify here, otherwise the user signature needs to be verified
        bool pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>>&& batch, bool validateOnReceive);

    protected:
        // As a slave node, need to verify the block before can endorse the block.
        void validateUnorderedBlock(zmq::message_t&& raw);

    public:
        [[nodiscard]] inline bool validateUserRequest(const proto::Envelop& envelop) const {
            auto& payload = envelop.getPayload();
            auto& signature = envelop.getSignature();
            const auto key = _bccsp->GetKey(signature.ski);
            if (key == nullptr) {
                LOG(WARNING) << "Can not load key, ski: " << signature.ski;
                return false;
            }
            return key->Verify(signature.digest, payload.data(), payload.size());
        }

        [[nodiscard]] inline bool validateUserRequestHash(const proto::SignatureString& signature, const util::OpenSSLSHA256::digestType& hash) const {
            const auto key = _bccsp->GetKey(signature.ski);
            if (key == nullptr) {
                LOG(WARNING) << "Can not load key, ski: " << signature.ski;
                return false;
            }
            return key->VerifyRaw(signature.digest, hash.data(), hash.size());
        }

    protected:
        [[nodiscard]] std::optional<::util::OpenSSLED25519::digestType> OnSignMessage(const ::util::NodeConfigPtr& localNode, const std::string& message) const override;

        // The following functions are called by state machine
        bool OnVerifyProposal(::util::NodeConfigPtr localNode, const std::string& serializedHeader) override;

        bool OnDeliver(::util::NodeConfigPtr localNode, const std::string& context,
                       std::vector<::proto::Block::SignaturePair>&& signatures) override;

        void OnLeaderStart(::util::NodeConfigPtr localNode, int sequence) override;

        void OnLeaderChange(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence) override;

        // Call by the leader only
        std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr localNode, int sequence, const std::string& context) override;

    protected:

    private:
        std::atomic<bool> _running;
        // Check if the node is leader
        std::shared_mutex _isLeaderMutex;
        bool _isLeader = false;
        std::unique_ptr<ContentSender> _sender;
        std::unique_ptr<ContentReceiver> _receiver;
        // Used when the node become follower, receive txn batch from leader, using zeromq
        std::unique_ptr<PBFTBlockCache> _blockCache;
        // Used when the node become leader, receive txn batch from user and enqueue to queue
        // TODO: consider limit the size of the queue
        util::BlockingConcurrentQueue<std::shared_ptr<::proto::Block>> _requestBatchQueue;
        // BCCSP and thread pool
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
        // For saving delivered blocks
        std::function<bool(std::shared_ptr<::proto::Block> block, ::util::NodeConfigPtr localNode)> _deliverCallback;
        // This Function is called inside a lock, thus thread safe
        std::function<void(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence)> _leaderChangeCallback;
    };
}