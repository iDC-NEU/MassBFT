//
// Created by user on 23-8-28.
//

#include "peer/consensus/pbft/local_consensus.h"
#include "peer/consensus/pbft/request_replicator.h"
#include "peer/consensus/pbft/pbft_block_cache.h"
#include "common/proof_generator.h"

namespace peer::consensus::v2 {
    LocalConsensus::LocalConsensus(Config config)
            : _config(std::move(config)), _running(false) {
        _blockCache = std::make_unique<PBFTBlockCache>();
        _requestReplicator = std::make_unique<RequestReplicator>(RequestReplicator::Config{_config.timeoutMs, _config.maxBatchSize});
        _requestReplicator->setBatchCallback([this](auto&& item) {
            return this->pushUnorderedBlock(std::forward<decltype(item)>(item));
        });
    }

    bool LocalConsensus::pushUnorderedBlock(std::vector<std::unique_ptr<proto::Envelop>> batch) {
        std::shared_ptr<::proto::Block> block(new proto::Block);
        block->body.userRequests = std::move(batch);
        const bool isLeader = _isLeader.load(std::memory_order_acquire);
        auto updateBlockDataHash = [&]() -> bool {
            util::ValidateHandleType validateHandle = nullptr;
            if (!isLeader) {    // follower validate the signatures
                // const long totalSig = _totalSig.load(std::memory_order_acquire);

                validateHandle = [this](const auto& signature, const auto& hash)->bool {
                    _totalSig = _totalSig+1;
                    if(_totalSig % _sigInterval == 0) {
                      const auto key = _bccsp->GetKey(signature.ski);
                      if (key == nullptr) {
                        LOG(WARNING) << "Can not load key, ski: " << signature.ski;
                        return false;
                      }
                      bool result = key->VerifyRaw(signature.digest, hash.data(), hash.size());
                      // 发送验证结果到上层服务器
                      return result;
                    } else {
                      return true;
                    }

                };
            }
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
            return true;
        };

        if (!updateBlockDataHash()) {
            return false;
        }
        // leader push the block into queue
        if (isLeader) {
            if (!_requestBatchQueue.enqueue(block)) {
                CHECK(false) << "Queue max size achieve!";
            }
        }
        _blockCache->storeCachedBlock(std::move(block));
        return true;
    }

    std::unique_ptr<::proto::Block::SignaturePair> LocalConsensus::OnSignProposal(const util::NodeConfigPtr &, const std::string &) {
        auto sig = _signatureCache.pop();
        // sig->first = message;
        return sig;
    }

    bool LocalConsensus::OnVerifyProposal(const util::NodeConfigPtr &localNode, const std::string &serializedHeader) {
        proto::Block::Header header;
        if (!header.deserializeFromString(serializedHeader)) {
            return false;
        }
        DLOG(INFO) << "Verify Block, groupId: " << localNode->groupId << " blk number:" << header.number;
        if (!_blockCache->isDeliveredBlockHeaderValid(header)) {
            return false;
        }
        // create signed message
        const auto key = _bccsp->GetKey(localNode->ski);
        if (key == nullptr || !key->Private()) {
            LOG(WARNING) << "Can not load key.";
            return false;
        }
        // sign the message with the private key
        auto signature = key->Sign(serializedHeader.data(), serializedHeader.size());
        if (signature == std::nullopt) {
            LOG(WARNING) << "Sign message failed.";
            return false;
        }

        auto pair = std::make_unique<::proto::Block::SignaturePair>();
        pair->second.ski = localNode->ski;
        pair->second.digest = *signature;
        _signatureCache.push(std::move(pair));

        // Find the target block in block pool (wait timed),
        // the other thread will validate the block,
        // so if we find the block, we can return safely.
        auto block = _blockCache->loadCachedBlock(header.dataHash, _config.timeoutMs*10);
        if (block == nullptr) {
            return false;
        }
        DCHECK(block->header.dataHash == header.dataHash);
        block->header = header;
        return true;
    }

    bool LocalConsensus::OnDeliver(::util::NodeConfigPtr localNode,
                                   const std::string &context,
                                   std::vector<::proto::Block::SignaturePair> &&signatures) {
        proto::Block::Header header;
        if (!header.deserializeFromString(context)) {
            return false;
        }
        auto block = _blockCache->eraseCachedBlock(header.dataHash);
        CHECK(block != nullptr) << "Block mut be not null!" << util::OpenSSLSHA256::toString(header.dataHash);
        // serialize block here (do not serialize signature)
        {
            auto serializedBlock = std::make_unique<std::string>();
            serializedBlock->reserve(100 * 1024);
            auto posList = block->serializeToString(serializedBlock.get());
            if (!posList.valid) {
                LOG(WARNING) << "Serialize block failed!";
            }
            block->setSerializedMessage(std::move(serializedBlock));
        }
        block->metadata.consensusSignatures = std::move(signatures);

        // validate the block signature
        // for (const auto& it: block->metadata.consensusSignatures) {
        //     const auto key = _bccsp->GetKey(it.second.ski);
        //     CHECK(key->Verify(it.second.digest, context.data(), context.size()));
        // }

        DLOG(INFO) << "Block delivered by BFT, groupId: " << localNode->groupId << " blk number:" << block->header.number;
        // local consensus complete
        if (_deliverCallback != nullptr) {
            _deliverCallback(block, std::move(localNode));
        }
        _blockCache->setBlockDelivered(std::move(block));
        return true;
    }

    void LocalConsensus::OnLeaderStart(::util::NodeConfigPtr localNode, int) {
        _blockCache->setBlockProposed(_blockCache->getBlockDelivered());
        _signatureCache.reset();
        _isLeader = true;
        auto portInfo = _config.getNodeInfo(localNode->nodeId);
        CHECK(portInfo != nullptr) << "Can not find node!";
        _requestReplicator->startLeader(_config.userRequestPort, portInfo->port);
    }

    void LocalConsensus::OnLeaderChange(::util::NodeConfigPtr, ::util::NodeConfigPtr newLeaderNode, int) {
        _isLeader = false;
        _blockCache->setBlockProposed(nullptr); // clear the state
        _signatureCache.reset();
        auto portInfo = _config.getNodeInfo(newLeaderNode->nodeId);
        _requestReplicator->startFollower(portInfo->nodeConfig->priIp, portInfo->port);
    }

    std::optional<std::string> LocalConsensus::OnRequestProposal(::util::NodeConfigPtr localNode, int, const std::string &) {
        if (!_isLeader) {
            return std::nullopt;
        }
        std::shared_ptr<::proto::Block> block;
        while (!_requestBatchQueue.wait_dequeue_timed(block, std::chrono::milliseconds(_config.timeoutMs*10))) {
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
        // Sign the serialized block header is enough, return the header only
        std::string serializedHeader;
        if (!block->header.serializeToString(&serializedHeader)) {
            return std::nullopt;
        }

        // LOG(INFO) << "Leader propose a block, hash: " << util::OpenSSLSHA256::toString(block->header.dataHash);
        const auto key = _bccsp->GetKey(localNode->ski);
        if (key == nullptr || !key->Private()) {
            LOG(WARNING) << "Can not load key.";
            return std::nullopt;
        }
        // sign the message with the private key
        auto signature = key->Sign(serializedHeader.data(), serializedHeader.size());
        if (signature == std::nullopt) {
            LOG(WARNING) << "Sign message failed.";
            return std::nullopt;
        }

        auto pair = std::make_unique<::proto::Block::SignaturePair>();
        pair->second.ski = localNode->ski;
        pair->second.digest = *signature;
        _signatureCache.push(std::move(pair));

        return serializedHeader;
    }

    bool LocalConsensus::checkAndStart() {
        if (_config.localId != 0) { // start follower
            auto portInfo = _config.getNodeInfo(0);
            _requestReplicator->startFollower(portInfo->nodeConfig->priIp, portInfo->port);
        }
        _running = true;
        return true;
    }

    LocalConsensus::~LocalConsensus() {
        _running = false;
    }

}