//
// Created by user on 23-8-3.
//

#pragma once

#include "common/parallel_merkle_tree.h"
#include "proto/block.h"

namespace util {
    using ValidateHandleType = std::function<bool(const proto::SignatureString& signature, const OpenSSLSHA256::digestType& hash)>;
    class UserRequestMTGenerator {
    public:
        [[nodiscard]] static std::unique_ptr<pmt::MerkleTree> GenerateMerkleTree(
                const std::vector<std::unique_ptr<proto::Envelop>>& userRequests,
                const ValidateHandleType& validateHandle,
                pmt::ModeType nodeType = pmt::ModeType::ModeProofGenAndTreeBuild,
                std::shared_ptr<util::thread_pool_light> wp = nullptr) {
            pmt::Config pmtConfig;
            pmtConfig.Mode = nodeType;
            pmtConfig.LeafGenParallel = true;
            if (userRequests.size() > 1024) {
                pmtConfig.RunInParallel = true;
            }
            std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
            blocks.reserve(userRequests.size());
            for (const auto & it : userRequests) {
                blocks.emplace_back(new EnvelopDataBlock(*it, validateHandle));
            }
            if (blocks.size() == 1) {   // special case: block has only 1 user request
                blocks.emplace_back(new EnvelopDataBlock(*userRequests[0], nullptr));
            }
            return pmt::MerkleTree::New(pmtConfig, blocks, std::move(wp));
        }

        static std::optional<pmt::Proof> GenerateProof(const pmt::MerkleTree& mt, const proto::Envelop& envelop) {
            EnvelopDataBlock mdb(envelop, nullptr);
            return mt.GenerateProof(mdb);
        }

        static bool ValidateProof(const proto::HashString &root, const pmt::Proof& proof, const proto::Envelop& envelop) {
            EnvelopDataBlock mdb(envelop, nullptr);
            auto ret = pmt::MerkleTree::Verify(mdb, proof, root);
            if (ret == std::nullopt || !*ret) {
                return false;
            }
            return true;
        }

    private:
        class EnvelopDataBlock: public pmt::DataBlock {
        public:
            explicit EnvelopDataBlock(
                    const proto::Envelop& envelop,
                    ValidateHandleType validateHandle)
                    : _envelop(envelop), _validateHandle(std::move(validateHandle)) { }

            [[nodiscard]] std::optional<pmt::HashString> Digest() const override {
                auto str = _envelop.getPayload();
                auto digest = util::OpenSSLSHA256::generateDigest(str.data(), str.size());
                if (digest == std::nullopt) {
                    LOG(ERROR) << "Can not generate digest.";
                    return std::nullopt;
                }
                if (_validateHandle && !_validateHandle(_envelop.getSignature(), *digest)) {
                    LOG(ERROR) << "Validate userRequests failed!";
                    return std::nullopt;
                }
                return digest;
            }

        private:
            const proto::Envelop& _envelop;
            ValidateHandleType _validateHandle;
        };
    };

    class ExecResultMTGenerator {
    public:
        [[nodiscard]] static std::unique_ptr<pmt::MerkleTree> GenerateMerkleTree(
                const std::vector<std::unique_ptr<proto::TxReadWriteSet>>& txReadWriteSet,
                const std::vector<std::byte>& transactionFilter,
                pmt::ModeType nodeType = pmt::ModeType::ModeProofGenAndTreeBuild,
                std::shared_ptr<util::thread_pool_light> wp = nullptr) {
            if (txReadWriteSet.size() != transactionFilter.size()) {
                return nullptr;
            }
            pmt::Config pmtConfig;
            pmtConfig.Mode = nodeType;
            pmtConfig.LeafGenParallel = true;
            if (txReadWriteSet.size() > 1024) {
                pmtConfig.RunInParallel = true;
            }
            std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
            blocks.reserve(txReadWriteSet.size());
            for (int i=0; i<(int)txReadWriteSet.size(); i++) {
                blocks.emplace_back(new ExecResultBlock(*txReadWriteSet[i], transactionFilter[i]));
            }
            if (blocks.size() == 1) {
                blocks.emplace_back(new ExecResultBlock(*txReadWriteSet[0], transactionFilter[0]));
            }
            return pmt::MerkleTree::New(pmtConfig, blocks, std::move(wp));
        }

        static std::optional<pmt::Proof> GenerateProof(const pmt::MerkleTree& mt,
                                                       const proto::TxReadWriteSet& rwSet,
                                                       const std::byte& filter) {
            ExecResultBlock mdb(rwSet, filter);
            return mt.GenerateProof(mdb);
        }

        static bool ValidateProof(const proto::HashString &root,
                                  const pmt::Proof& proof,
                                  const proto::TxReadWriteSet& rwSet,
                                  const std::byte& filter) {
            ExecResultBlock mdb(rwSet, filter);
            auto ret = pmt::MerkleTree::Verify(mdb, proof, root);
            if (ret == std::nullopt || !*ret) {
                return false;
            }
            return true;
        }

    private:
        class ExecResultBlock: public pmt::DataBlock {
        public:
            explicit ExecResultBlock(
                    const proto::TxReadWriteSet& rwSet,
                    const std::byte& filter)
                    : rwSet(rwSet), filter(filter) { }

            [[nodiscard]] std::optional<pmt::HashString> Digest() const override {
                // serialize envelop first
                std::string str;
                str.reserve(1024);
                zpp::bits::out out(str);
                if(failure(out(rwSet, filter))) {
                    LOG(ERROR) << "Serialize rwSet failed!";
                    return std::nullopt;
                }
                auto digest = util::OpenSSLSHA256::generateDigest(str.data(), str.size());
                if (digest == std::nullopt) {
                    LOG(ERROR) << "Can not generate digest.";
                    return std::nullopt;
                }
                return digest;
            }

        private:
            const proto::TxReadWriteSet& rwSet;
            const std::byte& filter;
        };
    };

    bool serializeToString(const pmt::Proof& proof, std::string& ret, int startPos = 0);

    bool deserializeFromString(const std::string& raw, pmt::Proof& ret, std::vector<pmt::HashString>& container, int startPos = 0);
}