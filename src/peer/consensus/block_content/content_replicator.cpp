//
// Created by user on 23-7-24.
//

#include "peer/consensus/block_content/content_replicator.h"
#include "peer/consensus/block_content/pbft_block_cache.h"

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
                _clients.emplace_back(util::ZMQInstance::NewClient<zmq::socket_type::push>(it->priAddr(), it->port));
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

    ContentReplicator::ContentReplicator(const std::vector<std::shared_ptr<util::ZMQInstanceConfig>> &targetNodes, int localId)
            : _running(false) {
        _sender = std::make_unique<ContentSender>(targetNodes, localId);
        _receiver = std::make_unique<ContentReceiver>(targetNodes[localId]->port);
        _receiver->setCallback([this](auto&& raw){ this->validateUnorderedBlock(std::forward<decltype(raw)>(raw)); });
        _blockCache = std::unique_ptr<PBFTBlockCache>(new PBFTBlockCache());
    }

    ContentReplicator::~ContentReplicator() = default;

    bool ContentReplicator::checkAndStart() {
        _running = true;
        return _receiver->checkAndStart();
    }

    bool ContentReplicator::pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>> &&batch, bool skipValidate) {
        std::shared_ptr<::proto::Block> block(new proto::Block);
        block->body.userRequests = std::move(batch);
        if (skipValidate) {
            auto rawBlock = std::make_unique<std::string>();
            auto pos = block->serializeToString(rawBlock.get());
            auto ret = util::OpenSSLSHA256::generateDigest(rawBlock->data() + pos.bodyPos, pos.execResultPos - pos.bodyPos);
            if (ret == std::nullopt) {
                LOG(WARNING) << "Signature generate failed.";
                return false;
            }
            block->header.dataHash = *ret;
            block->setSerializedMessage(std::move(rawBlock));
            if (!_requestBatchQueue.enqueue(std::move(block))) {
                CHECK(false) << "Queue max size achieve!";
            }
            return true;
        }
        // thread pool validate user requests
        bool success = true;
        auto numRoutines = (int) _threadPoolForBCCSP->get_thread_count();
        bthread::CountdownEvent countdown(numRoutines);
        for (auto i = 0; i < numRoutines; i++) {
            _threadPoolForBCCSP->push_task([&, start = i] {
                auto &userRequests = block->body.userRequests;
                for (int j = start; j < (int) userRequests.size(); j += numRoutines) {
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
        auto ret = util::OpenSSLSHA256::generateDigest(rawBlock->data() + pos.bodyPos, pos.execResultPos - pos.bodyPos);
        countdown.wait();
        if (!success || ret == std::nullopt) {
            LOG(WARNING) << "Signature generate failed.";
            return false;
        }
        block->header.dataHash = *ret;
        block->setSerializedMessage(std::move(rawBlock));
        // Block number and previous data hash are set in OnRequestProposal as leader
        if (!_requestBatchQueue.enqueue(std::move(block))) {
            CHECK(false) << "Queue max size achieve!";
        }
        return true;
    }

    void ContentReplicator::validateUnorderedBlock(zmq::message_t &&raw) {
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
        _blockCache->storeCachedBlock(std::move(block));
    }

    auto ContentReplicator::OnSignMessage(const util::NodeConfigPtr &localNode, const std::string &message) const -> std::optional<::util::OpenSSLED25519::digestType> {
        // Get the private key of this node
        const auto key = _bccsp->GetKey(localNode->ski);
        if (key == nullptr || !key->Private()) {
            LOG(WARNING) << "Can not load key.";
            return std::nullopt;
        }
        // sign the message with the private key
        return key->Sign(message.data(), message.size());
    }

    bool ContentReplicator::OnVerifyProposal(::util::NodeConfigPtr, const std::string &serializedHeader) {
        proto::Block::Header header;
        if (!header.deserializeFromString(serializedHeader)) {
            return false;
        }
        if (!_blockCache->isDeliveredBlockHeaderValid(header)) {
            return false;
        }
        // Find the target block in block pool (wait timed),
        // the other thread will validate the block,
        // so if we find the block, we can return safely.
        auto block = _blockCache->loadCachedBlock(header.dataHash, 500); // wait for 500 ms
        if (block == nullptr) {
            return false;   // timeout
        }
        DCHECK(block->header.dataHash == header.dataHash);
        return true;
    }

    bool ContentReplicator::OnDeliver(::util::NodeConfigPtr localNode, const std::string &context,
                                      std::vector<::proto::Block::SignaturePair> &&signatures) {
        proto::Block::Header header;
        if (!header.deserializeFromString(context)) {
            return false;
        }
        auto block = _blockCache->eraseCachedBlock(header.dataHash);
        CHECK(block != nullptr) << "Block mut be not null!" << util::OpenSSLSHA256::toString(header.dataHash);
        block->metadata.consensusSignatures = std::move(signatures);
        DLOG(INFO) << "Block delivered by BFT, groupId: " << localNode->groupId << " blk number:" << block->header.number;
        // local consensus complete
        if (_deliverCallback != nullptr) {
            _deliverCallback(block, std::move(localNode));
        }
        _blockCache->setBlockDelivered(std::move(block));
        return true;
    }

    void ContentReplicator::OnLeaderStart(::util::NodeConfigPtr localNode, int sequence) {
        std::unique_lock lock(_isLeaderMutex);
        _blockCache->setBlockProposed(_blockCache->getBlockDelivered());
        _isLeader = true;
        if (_leaderChangeCallback) {
            _leaderChangeCallback(std::move(localNode), nullptr, sequence);
        }
    }

    void ContentReplicator::OnLeaderChange(::util::NodeConfigPtr localNode, ::util::NodeConfigPtr newLeaderNode, int sequence) {
        std::unique_lock lock(_isLeaderMutex);
        _blockCache->setBlockProposed(nullptr); // clear the state
        _isLeader = false;
        if (_leaderChangeCallback) {
            _leaderChangeCallback(std::move(localNode), std::move(newLeaderNode), sequence);
        }
    }

    std::optional<std::string> ContentReplicator::OnRequestProposal(::util::NodeConfigPtr localNode, int, const std::string &) {
        std::shared_lock lock(_isLeaderMutex);
        if (!_isLeader) {
            return std::nullopt;
        }
        std::shared_ptr<::proto::Block> block;
        while (!_requestBatchQueue.wait_dequeue_timed(block,  std::chrono::seconds(5))) {
            if (!_running) {
                LOG(INFO) << "The rpc instance is not running, return.";
                return std::nullopt;
            }
        }
        if (block == nullptr) {
            return std::nullopt;
        }
        _blockCache->updateBlockHeaderWithProposedBlock(block->header);
        DLOG(INFO) << "Leader of local group " << localNode->groupId << " created a block, number: " << block->header.number;
        _blockCache->setBlockProposed(block);
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
        _blockCache->storeCachedBlock(std::move(block));
        // Sign the serialized block header is enough, return the header only
        return serializedBlock->substr(pos.headerPos, pos.bodyPos-pos.headerPos);
    }

}