//
// Created by user on 23-7-24.
//

#pragma once

#include "proto/block.h"
#include "bthread/butex.h"
#include "common/phmap.h"
#include <memory>

namespace peer::consensus {
    class PBFTBlockCache {
    public:
        PBFTBlockCache() {
            _cache.first = bthread::butex_create_checked<butil::atomic<int>>();
        }

        ~PBFTBlockCache() {
            bthread::butex_destroy(_cache.first);
        }
        // returned block may be nullptr
        std::shared_ptr<proto::Block> loadCachedBlock(const proto::HashString& hash, int timeoutMs) {
            auto timeout = butil::milliseconds_from_now(timeoutMs);
            while(true) {
                auto currentBlockCount = _cache.first->load(std::memory_order_acquire);
                std::shared_ptr<proto::Block> block = nullptr;
                _cache.second.if_contains(hash, [&block](const auto& v) { block = v.second; });
                if (block != nullptr) {
                    return block;
                }
                if (bthread::butex_wait(_cache.first, currentBlockCount, &timeout) != 0 && errno == ETIMEDOUT) {
                    LOG(ERROR) << "Can not get the block in specific timeout!";
                    return nullptr;
                }
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

        // validator
        void setBlockDelivered(auto&& block) { _deliveredBlock = std::forward<decltype(block)>(block); }

        [[nodiscard]] auto getBlockDelivered() const { return _deliveredBlock; }

        [[nodiscard]] bool isDeliveredBlockHeaderValid(const proto::Block::Header& header) {
            if (_deliveredBlock == nullptr) {
                return true;    // 1st block
            }
            auto exceptPreviousHash = CalculatePreviousHash(_deliveredBlock->header);
            if (exceptPreviousHash == std::nullopt) {
                return false;
            }

            if (*exceptPreviousHash != header.previousHash ||
                _deliveredBlock->header.number + 1 != header.number) {
                LOG(ERROR) << "Expect number: " << _deliveredBlock->header.number + 1 << ", got: " << header.number;
                LOG(ERROR) << "Expect prevHash: " << util::OpenSSLSHA256::toString(_deliveredBlock->header.dataHash)
                           << ", got: " << util::OpenSSLSHA256::toString(header.previousHash);
                return false;
            }
            return true;
        }

        // leader
        void setBlockProposed(auto&& block) { _proposedLastBlock = std::forward<decltype(block)>(block); }

        void updateBlockHeaderWithProposedBlock(proto::Block::Header& header) {
            if (_proposedLastBlock == nullptr) {
                return;
            }
            header.previousHash = CalculatePreviousHash(_proposedLastBlock->header).value_or(proto::HashString{});
            header.number = _proposedLastBlock->header.number + 1;
        }

    protected:
        inline static std::optional<proto::HashString> CalculatePreviousHash(const proto::Block::Header& header) {
            std::string serializedBlockHeader;
            serializedBlockHeader.reserve(128);
            if (!header.serializeToString(&serializedBlockHeader, 0)) {
                return std::nullopt;
            }
            return util::OpenSSLSHA256::generateDigest(serializedBlockHeader.data(), serializedBlockHeader.size());
        }

    private:
        std::pair<butil::atomic<int>*, util::MyFlatHashMap<proto::HashString, std::shared_ptr<proto::Block>, std::mutex>> _cache;

        std::shared_ptr<::proto::Block> _deliveredBlock;
        std::shared_ptr<::proto::Block> _proposedLastBlock;
    };
}