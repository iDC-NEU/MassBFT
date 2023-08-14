//
// Created by user on 23-8-3.
//

#pragma once

#include "common/parallel_merkle_tree.h"
#include "proto/block.h"

namespace util {
    class ProofGenerator {
    public:
        explicit ProofGenerator(const proto::Block::Body& body) {
            posList.resize(body.userRequests.size());
            serializedBody.reserve(body.userRequests.size() * 256);
            zpp::bits::out out(serializedBody);
            for (int i=0; i<(int)body.userRequests.size(); i++) {
                if(failure(out(body.userRequests[i]->getPayload()))) {
                    CHECK(false) << "Serialize userRequests failed!";
                }
                posList[i] = (int)out.position();
            }
        }

        explicit ProofGenerator(const proto::Block::ExecuteResult& executeResult) {
            posList.resize(executeResult.txReadWriteSet.size());
            serializedBody.reserve(executeResult.txReadWriteSet.size() * 384);
            zpp::bits::out out(serializedBody);
            for (int i=0; i<(int)executeResult.txReadWriteSet.size(); i++) {
                // serialize std::unique_ptr<Envelop>
                if(failure(out(*executeResult.txReadWriteSet[i], executeResult.transactionFilter[i]))) {
                    CHECK(false) << "Serialize executeResult failed!";
                }
                posList[i] = (int)out.position();
            }
        }

        [[nodiscard]] std::unique_ptr<pmt::MerkleTree> generateMerkleTree(
                pmt::ModeType nodeType = pmt::ModeType::ModeProofGenAndTreeBuild,
                std::shared_ptr<util::thread_pool_light> wp = nullptr) const {
            if (posList.empty()) {
                return nullptr;
            }
            pmt::Config pmtConfig;
            pmtConfig.Mode = nodeType;
            if (posList[0] > 1024) {
                pmtConfig.LeafGenParallel = true;
            }
            if (posList.size() > 1024) {
                pmtConfig.RunInParallel = true;
            }
            std::string_view bodySV = serializedBody;
            std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
            for (int i=0; i<(int)posList.size(); i++) {
                if (i == 0) {
                    blocks.emplace_back(new UserRequestDataBlock(bodySV.substr(0, posList[i])));
                    continue;
                }
                blocks.emplace_back(new UserRequestDataBlock(bodySV.substr(posList[i-1], posList[i])));
            }
            if (blocks.size() == 1) {   // special case: block has only 1 user request
                blocks.emplace_back(new UserRequestDataBlock(bodySV.substr(0, posList[0])));
            }
            return pmt::MerkleTree::New(pmtConfig, blocks, std::move(wp));
        }

        static std::optional<pmt::Proof> GenerateProof(const pmt::MerkleTree& mt, const std::string& dataBlock) {
            UserRequestDataBlock mdb(dataBlock);
            return mt.GenerateProof(mdb);
        }

        static bool ValidateProof(const proto::HashString &root, const pmt::Proof& proof, const std::string& dataBlock) {
            UserRequestDataBlock mdb(dataBlock);
            auto ret = pmt::MerkleTree::Verify(mdb, proof, root);
            if (ret == std::nullopt || !*ret) {
                return false;
            }
            return true;
        }

    private:
        std::vector<int> posList;
        std::string serializedBody;

    private:
        class UserRequestDataBlock: public pmt::DataBlock {
        public:
            explicit UserRequestDataBlock(std::string_view userRequest) : _userRequest(userRequest) { }

            [[nodiscard]] pmt::ByteString Serialize() const override {
                return _userRequest;
            }

        private:
            std::string_view _userRequest;
        };
    };

    bool serializeToString(const pmt::Proof& proof, std::string& ret, int startPos = 0);

    bool deserializeFromString(const std::string& raw, pmt::Proof& ret, std::vector<pmt::HashString>& container, int startPos = 0);
}