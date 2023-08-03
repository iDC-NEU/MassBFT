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
            if (!body.serializeForProofGen(posList, serializedBody)) {
                CHECK(false) << "Serialize proof failed!";
            }
        }

        explicit ProofGenerator(const proto::Block::ExecuteResult& executeResult) {
            if (!executeResult.serializeForProofGen(posList, serializedBody)) {
                CHECK(false) << "Serialize proof failed!";
            }
        }

        auto generateMerkleTree() {
            pmt::Config pmtConfig;
            pmtConfig.Mode = pmt::ModeType::ModeProofGenAndTreeBuild;
            if (posList[0] > 1024) {
                pmtConfig.LeafGenParallel = true;
            }
            if (posList.size() > 1024) {
                pmtConfig.RunInParallel = true;
            }
            std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
            for (int i=0; i<(int)posList.size(); i++) {
                if (i == 0) {
                    blocks.emplace_back(new UserRequestDataBlock({serializedBody.data() + 0, static_cast<size_t>(posList[i])}));
                }
                blocks.emplace_back(new UserRequestDataBlock({serializedBody.data() + posList[i-1], static_cast<size_t>(posList[i])}));
            }
            return pmt::MerkleTree::New(pmtConfig, blocks, wp.get());
        }

    private:
        std::vector<int> posList;
        std::string serializedBody;
        std::unique_ptr<util::thread_pool_light> wp  = std::make_unique<util::thread_pool_light>(
                std::min((int)std::thread::hardware_concurrency(), 2), "pr_gn_wk");

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
}