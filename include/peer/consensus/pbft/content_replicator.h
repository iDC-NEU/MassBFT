//
// Created by user on 23-3-23.
//

#pragma once

#include "peer/consensus/pbft/pbft_state_machine.h"
#include "proto/block.h"
#include "common/zeromq.h"
#include "common/property.h"
#include "common/bccsp.h"
#include "common/phmap.h"
#include "common/timer.h"
#include "common/thread_pool_light.h"
#include "common/concurrent_queue.h"

#include "bthread/butex.h"
#include "bthread/countdown_event.h"

namespace peer::consensus {
    class ContentReceiver {
    public:
        explicit ContentReceiver(int port) :_port(port) { }

        ~ContentReceiver() {
            if (_server) { _server->shutdown(); }
            if (_thread) { _thread->join(); }
        }

        void setCallback(auto callback) { _callback = std::move(callback); }

        bool checkAndStart() {
            _server = util::ZMQInstance::NewServer<zmq::socket_type::pull>(_port);
            if (!_server) {
                return false;
            }
            _thread = std::make_unique<std::thread>(run, this);
            return true;
        }

    protected:
        static void* run(void* ptr) {
            pthread_setname_np(pthread_self(), "pbft_ctn_rec");
            auto* instance=static_cast<ContentReceiver*>(ptr);
            while(true) {
                auto ret = instance->_server->receive();
                if (ret == std::nullopt) {
                    LOG(ERROR) << "Receive PBFT content failed!";
                    break;
                }
                instance->_callback(std::move(*ret));
            }
            return nullptr;
        }

    private:
        const int _port;
        std::unique_ptr<std::thread> _thread;
        std::unique_ptr<util::ZMQInstance> _server;
        std::function<void(zmq::message_t&& raw)> _callback;
    };

    class ContentSender {
    public:
        ContentSender(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& targetNodes, int localId) {
            _clients.reserve(targetNodes.size());
            for(const auto& it:targetNodes) {
                if (it->nodeConfig->nodeId == localId) {
                    continue;
                }
                _clients.emplace_back(util::ZMQInstance::NewClient<zmq::socket_type::push>(it->addr(), it->port));
            }
        }

        void send(const std::string& content) {
            for (auto& it: _clients) {
                it->send(content);
            }
        }

    private:
        std::vector<std::unique_ptr<util::ZMQInstance>> _clients;
    };

    class ContentReplicator: public PBFTStateMachine {
    public:
        ContentReplicator(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>>& targetNodes, int localId, int64_t timeoutMs=100)
                : _verifyProposalTimeout(timeoutMs) {
            _sender = std::make_unique<ContentSender>(targetNodes, localId);
            _receiver = std::make_unique<ContentReceiver>(targetNodes[localId]->port);
            _receiver->setCallback([this](auto&& raw){ this->validateUnorderedBlock(std::forward<decltype(raw)>(raw)); });
            _cache.first = bthread::butex_create_checked<butil::atomic<int>>();
        }

        ~ContentReplicator() override { bthread::butex_destroy(_cache.first); }

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
        bool checkAndStart() {
            return _receiver->checkAndStart();
        }

        // If the user request has been verified before (pessimistic verification),
        // there is no need to re-verify here, otherwise the user signature needs to be verified
        template<bool validated>
        bool pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>>&& batch) {
            std::shared_ptr<::proto::Block> block(new proto::Block);
            block->body.userRequests = std::move(batch);
            if (validated) {
                auto rawBlock = std::make_unique<std::string>();
                auto pos = block->serializeToString(rawBlock.get());
                auto ret = util::OpenSSLSHA256::generateDigest(rawBlock->data()+pos.bodyPos, pos.execResultPos-pos.bodyPos);
                if (ret == std::nullopt) {
                    LOG(WARNING) << "Signature generate failed.";
                    return false;
                }
                block->header.dataHash = *ret;
                block->setSerializedMessage(std::move(rawBlock));
                _requestBatchQueue.enqueue(std::move(block));
                return true;
            }
            // thread pool validate user requests
            bool success = true;
            auto numRoutines = (int)_threadPoolForBCCSP->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPoolForBCCSP->push_task([&, start=i]{
                    auto& userRequests = block->body.userRequests;
                    for (int j = start; j < (int)userRequests.size(); j += numRoutines) {
                        if (!validateUserRequest(*userRequests[j])) {
                            success = false;
                            break;
                        }
                    }
                    countdown.signal();
                });
            }
            // main thread generate data hash
            auto rawBlock = std::make_unique<std::string>();
            auto pos = block->serializeToString(rawBlock.get());
            auto ret = util::OpenSSLSHA256::generateDigest(rawBlock->data()+pos.bodyPos, pos.execResultPos-pos.bodyPos);
            countdown.wait();
            if (!success || ret == std::nullopt) {
                LOG(WARNING) << "Signature generate failed.";
                return false;
            }
            block->header.dataHash = *ret;
            block->setSerializedMessage(std::move(rawBlock));
            // Block number and previous data hash are set in OnRequestProposal as leader
            _requestBatchQueue.enqueue(std::move(block));
            return true;
        }

    protected:
        // As a slave node, need to verify the block before can endorse the block.
        void validateUnorderedBlock(zmq::message_t&& raw) {
            std::shared_ptr<::proto::Block> block(new proto::Block);
            auto pos = block->deserializeFromString(raw.to_string());
            if (!pos.valid) {
                LOG(WARNING) << "Can not deserialize block.";
                return;
            }
            bool success = true;
            auto numRoutines = (int)_threadPoolForBCCSP->get_thread_count();
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                _threadPoolForBCCSP->push_task([&, start=i]{
                    auto& userRequests = block->body.userRequests;
                    for (int j = start; j < (int)userRequests.size(); j += numRoutines) {
                        if (!validateUserRequest(*userRequests[j])) {
                            success = false;
                            break;
                        }
                    }
                    countdown.signal();
                });
            }
            // main thread calculate data hash
            auto rawBlock = block->getSerializedMessage();
            auto digest = util::OpenSSLSHA256::generateDigest(rawBlock->data()+pos.bodyPos, pos.execResultPos-pos.bodyPos);
            countdown.wait();
            if (!success || digest != block->header.dataHash) {
                LOG(WARNING) << "Signature validate failed.";
                return;
            }
            // add block to cache
            // LOG(INFO) << "Follower store a block, hash: " << util::OpenSSLSHA256::toString(*digest);
            storeCachedBlock(std::move(block));
        }

    public:
        [[nodiscard]] bool validateUserRequest(const proto::Envelop& envelop) const {
            auto& payload = envelop.getPayload();
            auto& signature = envelop.getSignature();
            const auto key = _bccsp->GetKey(signature.ski);
            if (key == nullptr) {
                LOG(WARNING) << "Can not load key.";
                return false;
            }
            return key->Verify(signature.digest, payload.data(), payload.size());
        }

    protected:
        [[nodiscard]] std::optional<::util::OpenSSLED25519::digestType> OnSignMessage(const ::util::NodeConfigPtr& localNode, const std::string& message) const override {
            // Get the private key of this node
            const auto key = _bccsp->GetKey(localNode->ski);
            if (key == nullptr || !key->Private()) {
                LOG(WARNING) << "Can not load key.";
                return std::nullopt;
            }
            // sign the message with the private key
            return key->Sign(message.data(), message.size());
        }

        // The following functions are called by state machine
        bool OnVerifyProposal(::util::NodeConfigPtr localNode, const std::string& serializedHeader) override {
            proto::Block::Header header;
            if (!header.deserializeFromString(serializedHeader)) {
                return false;
            }
            if (_deliveredLastBlock != nullptr && (_deliveredLastBlock->header.dataHash != header.previousHash ||
                                                   header.number != _deliveredLastBlock->header.number + 1)) {
                LOG(ERROR) << "Expect number: " << _deliveredLastBlock->header.number + 1 << ", got: " << header.number;
                LOG(ERROR) << "Expect prevHash: " << util::OpenSSLSHA256::toString(_deliveredLastBlock->header.dataHash)
                           << ", got: " << util::OpenSSLSHA256::toString(header.previousHash);
                return false;
            }
            // Find the target block in block pool (wait timed),
            // the other thread will validate the block,
            // so if we find the block, we can return safely.
            auto block = loadCachedBlock(header.dataHash);
            if (block == nullptr) {
                return false;
            }
            DCHECK(block->header.dataHash == header.dataHash);
            return true;
        }

        bool OnDeliver(::util::NodeConfigPtr localNode,
                       const std::string& context,
                       std::vector<::proto::SignatureString> signatures) override {
            proto::Block::Header header;
            if (!header.deserializeFromString(context)) {
                return false;
            }
            auto block = eraseCachedBlock(header.dataHash);
            CHECK(block != nullptr) << "Block mut be not null!" << util::OpenSSLSHA256::toString(header.dataHash);
            block->metadata.consensusSignatures = std::move(signatures);
            // local consensus complete
            if (_deliverCallback != nullptr) {
                _deliverCallback(block, std::move(localNode));
            }
            _deliveredLastBlock = std::move(block);
            return true;
        }

        void OnLeaderStart(::util::NodeConfigPtr localNode, int sequence) override {
            std::unique_lock lock(_isLeaderMutex);
            _proposedLastBlock = _deliveredLastBlock;
            _isLeader = true;
            if (_leaderChangeCallback) {
                _leaderChangeCallback(std::move(localNode), nullptr, sequence);
            }
        }

        void OnLeaderChange(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence) override {
            std::unique_lock lock(_isLeaderMutex);
            _proposedLastBlock = nullptr;
            _isLeader = false;
            if (_leaderChangeCallback) {
                _leaderChangeCallback(std::move(localNode), std::move(newLeaderNode), sequence);
            }
        }

        // Call by the leader only
        std::optional<std::string> OnRequestProposal(::util::NodeConfigPtr localNode, int sequence, const std::string& context) override {
            std::shared_lock lock(_isLeaderMutex);
            if (!_isLeader) {
                return std::nullopt;
            }
            std::shared_ptr<::proto::Block> block;
            _requestBatchQueue.wait_dequeue(block);
            if (block == nullptr) {
                return std::nullopt;
            }
            if (_proposedLastBlock != nullptr) {
                block->header.previousHash = _proposedLastBlock->header.dataHash;
                block->header.number = _proposedLastBlock->header.number + 1;
            }
            _proposedLastBlock = block;
            // LOG(INFO) << "request proposal, block number: " << block->header.number;
            auto serializedBlock = block->getSerializedMessage();
            proto::Block::PosList pos;
            if (serializedBlock == nullptr) {   // create new serialized block
                serializedBlock = std::make_unique<std::string>();
                pos = block->serializeToString(serializedBlock.get());
                if (!pos.valid) {
                    return std::nullopt;
                }
            } else {    // reuse serialized block
                pos = block->UpdateSerializedHeader(block->header, serializedBlock.get());
                if (!pos.valid) {
                    return std::nullopt;
                }
            }
            _sender->send(*serializedBlock);
            // add block to cache (as leader)
            // LOG(INFO) << "Leader store a block, hash: " << util::OpenSSLSHA256::toString(block->header.dataHash);
            storeCachedBlock(std::move(block));
            // Sign the serialized block header is enough, return the header only
            return serializedBlock->substr(pos.headerPos, pos.bodyPos-pos.headerPos);
        }

    protected:
        // returned block may be nullptr
        std::shared_ptr<proto::Block> loadCachedBlock(const proto::HashString& hash) {
            int64_t endTime = util::Timer::time_now_ms() + _verifyProposalTimeout;
            while(true) {
                auto span = endTime - util::Timer::time_now_ms();
                if (span < 0) {
                    LOG(ERROR) << "Can not get the block in specific timeout!";
                    return nullptr;
                }
                auto currentBlockCount = _cache.first->load(std::memory_order_acquire);
                std::shared_ptr<proto::Block> block = nullptr;
                _cache.second.if_contains(hash, [&block](const auto& v) { block = v.second; });
                if (block == nullptr) {
                    auto timeout = butil::milliseconds_to_timespec(span);
                    bthread::butex_wait(_cache.first, currentBlockCount, &timeout);
                    continue;
                }
                return block;
            }
        }

        void storeCachedBlock(std::shared_ptr<proto::Block> block) {
            _cache.second.insert_or_assign(block->header.dataHash, std::move(block));
            _cache.first->fetch_add(1, std::memory_order_release);
            bthread::butex_wake_all(_cache.first);
        }

        // returned block may be nullptr
        std::shared_ptr<proto::Block> eraseCachedBlock(const proto::HashString& hash) {
            std::shared_ptr<proto::Block> block = nullptr;
            _cache.second.erase_if(hash, [&block](auto& v) { block = v.second; return true; });
            return block;
        }

    private:
        const int64_t _verifyProposalTimeout; // ms
        // Check if the node is leader
        std::shared_mutex _isLeaderMutex;
        bool _isLeader = false;
        std::unique_ptr<ContentSender> _sender;
        std::unique_ptr<ContentReceiver> _receiver;
        // Used when the node become follower, receive txn batch from leader, using zeromq
        std::pair<butil::atomic<int>*, util::MyFlatHashMap<proto::HashString, std::shared_ptr<proto::Block>, std::mutex>> _cache;
        // Used when the node become leader, receive txn batch from user and enqueue to queue
        util::BlockingConcurrentQueue<std::shared_ptr<::proto::Block>, 5> _requestBatchQueue;
        // Set by OnRequestProposal or OnVerifyProposal
        std::shared_ptr<::proto::Block> _deliveredLastBlock;
        std::shared_ptr<::proto::Block> _proposedLastBlock;
        // BCCSP and thread pool
        std::shared_ptr<util::BCCSP> _bccsp;
        std::shared_ptr<util::thread_pool_light> _threadPoolForBCCSP;
        // For saving delivered blocks
        std::function<bool(std::shared_ptr<::proto::Block> block, ::util::NodeConfigPtr localNode)> _deliverCallback;
        // This Function is called inside a lock, thus thread safe
        std::function<void(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence)> _leaderChangeCallback;
    };
}