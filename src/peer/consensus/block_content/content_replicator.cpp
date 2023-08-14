//
// Created by user on 23-7-24.
//

#include "peer/consensus/block_content/content_replicator.h"
#include "peer/consensus/block_content/pbft_block_cache.h"
#include "common/proof_generator.h"

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
        _blockCache = std::make_unique<PBFTBlockCache>();
    }

    ContentReplicator::~ContentReplicator() = default;

    bool ContentReplicator::checkAndStart() {
        _running = true;
        return _receiver->checkAndStart();
    }

    bool ContentReplicator::pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>> &&batch, bool validateOnReceive) {
        std::shared_ptr<::proto::Block> block(new proto::Block);
        block->body.userRequests = std::move(batch);
        util::ValidateHandleType validateHandle = nullptr;
        if (validateOnReceive) {    // validate twice, not needed
            validateHandle = [this](const auto& signature, const auto& hash)->bool {
                return this->validateUserRequestHash(signature, hash);
            };
        }
        auto updateDataHashAndSerializeBlock = [&]() -> bool {
            // generate merkle root
            auto mt = util::UserRequestMTGenerator::GenerateMerkleTree(block->body.userRequests,
                                                                       validateHandle,
                                                                       pmt::ModeType::ModeProofGenAndTreeBuild,
                                                                       _threadPoolForBCCSP);
            if (mt == nullptr) {
                LOG(WARNING) << "Generate merkle tree failed.";
                return false;
            }
            block->header.dataHash = mt->getRoot();
            // generate serialized block
            std::string rawBlock;
            auto pos = block->serializeToString(&rawBlock);
            if (!pos.valid) {
                LOG(WARNING) << "Serialize block failed.";
                return false;
            }
            block->setSerializedMessage(std::move(rawBlock));
            return true;
        };

        if (!updateDataHashAndSerializeBlock()) {
            return false;
        }
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
        auto validateHandle = [this](const auto& signature, const auto& hash)->bool {
            return this->validateUserRequestHash(signature, hash);
        };
        // main thread validate merkle root
        auto mt = util::UserRequestMTGenerator::GenerateMerkleTree(
                block->body.userRequests,
                validateHandle,
                pmt::ModeType::ModeTreeBuild,
                _threadPoolForBCCSP);
        if (mt == nullptr) {
            LOG(WARNING) << "Generate merkle tree failed.";
            return;
        }
        if (block->header.dataHash != mt->getRoot()) {
            LOG(WARNING) << "Validate merkle tree failed"
                         << ", expect: " << util::OpenSSLSHA256 ::toString(block->header.dataHash)
                         << ", got: " << util::OpenSSLSHA256 ::toString(mt->getRoot());
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
        while (!_requestBatchQueue.wait_dequeue_timed(block, std::chrono::seconds(5))) {
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
