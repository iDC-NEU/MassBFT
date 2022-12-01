//
// Created by peng on 11/29/22.
//

#pragma once

#include "common/erasure_code.h"
#include "common/parallel_merkle_tree.h"
#include "common/thread_pool_light.h"
#include "rigtorp/MPMCQueue.h"
#include "gtl/phmap.hpp"
#include "glog/logging.h"

#include "zpp_bits.h"

#include <optional>
#include <string>
#include <memory>

namespace peer {

    class BlockFragmentGenerator {
    public:
        struct Config {
            int dataShardCnt;
            int parityShardCnt;
        };

        class Context {
        protected:
        class ContextDataBlock: public pmt::DataBlock {
        public:
            explicit ContextDataBlock(pmt::byteString dataView) :_dataView(dataView) {}

            [[nodiscard]] pmt::byteString Serialize() const override {
                return _dataView;
            }
        private:
            // this is not the actual data!
            pmt::byteString _dataView;
        };
        public:
            ~Context() = default;
            Context(const Context&) = delete;

            [[nodiscard]] int getFragmentCnt() const {
                return fragmentCnt;
            }

            // Invoke for validation
            // support concurrent validation
            // This func will throw runtime error
            bool validateAndDeserializeFragments(const pmt::hashString& root, std::string_view raw, int start, int end) {
                if (start<0 || start>=end || end>fragmentCnt) {
                    throw std::out_of_range("index out of range");    // out of range
                }
                auto in = zpp::bits::in(raw);
                // 1. read depth(compressed)
                int64_t depth = 0;
                in(depth).or_throw();

                for (auto i=start; i<end; i++) {
                    bool falseFlag = false;
                    if (!proofCacheGuard[i].compare_exchange_strong(falseFlag, true, std::memory_order_release, std::memory_order_relaxed)) {
                        continue;   // another thread is modifying this fragment
                    }
                    auto &currentProof = regenerateProofs[i];
                    auto &currentProofCache = proofCache[i];

                    currentProof.Siblings.reserve(depth);
                    // 2. write path, compressed proof size
                    int64_t currentProofSize = 0;
                    in(currentProof.Path, currentProofSize).or_throw();
                    // 3. read proofs
                    // resize to avoid invalid pointer
                    currentProofCache.resize(currentProofSize);
                    for (auto j = 0; j < currentProofSize; j++) {
                        in(currentProofCache[j]).or_throw();
                        currentProof.Siblings.push_back(&currentProofCache[j]);
                    }
                    if (i != start) {
                        const auto &previousProof = regenerateProofs[i - 1];
                        for (auto j = currentProofSize; j < depth; j++) {
                            currentProof.Siblings.push_back(previousProof.Siblings[j]);
                        }
                    }
                    // 4. read actual data
                    std::string_view fragment;
                    in(fragment).or_throw();
                    ContextDataBlock fragmentView(fragment);
                    // we have finished loading all proofs, verify them.
                    auto verifyResult = pmt::MerkleTree::Verify(fragmentView, currentProof, root);
                    if (verifyResult && *verifyResult) {
                        ecDecodeResult[i] = fragment;
                    } else {
                        LOG(ERROR) << "Verification failed";
                        return false;
                    }
                }
                return true;
            }

            // Invoke for validation
            // when enough pieces is found, we can regenerate message
            // No concurrent support, the caller must ensure ALL validateAndDeserializeFragments call is finished.
            // WARNING: EC hold the return value, it is not safe to use EC until you finished using the returned message
            // NOTE: need to resize message to the actual size (if using the go erasure code)
            std::optional<std::string_view> regenerateMessage() {
                std::vector<std::string_view> svListPartialView;
                for(const auto& dr:ecDecodeResult) {
                    svListPartialView.push_back(dr);
                }
                decodeResultHolder = ec->decode(svListPartialView);
                if (!decodeResultHolder) {
                    util::OpenSSLSHA256 hash;
                    for(const auto& pv: svListPartialView) {
                        hash.update(pv.data(), pv.size());
                    }
                    LOG(ERROR) << "Regenerate message failed, hash: " << util::OpenSSLSHA256::toString(*hash.final());
                    return std::nullopt;
                }
                return decodeResultHolder->getData();
            }

            // Invoke for generation
            // No concurrent support
            bool initWithMessage(std::string_view message) {
                if (mt != nullptr) {
                    return false;   // already inited
                }
                encodeResultHolder = ec->encode(message);
                if (encodeResultHolder == nullptr) {
                    return false;
                }
                auto ret = encodeResultHolder->getAll();
                if (!ret) {
                    return false;
                }
                std::vector<std::string_view> ecEncodeResult = std::move(*ret);
                pmtConfig.Mode=pmt::ModeType::ModeProofGenAndTreeBuild;
                if (ecEncodeResult[0].size() > 1024) {
                    pmtConfig.LeafGenParallel=true;
                }
                std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
                blocks.reserve(ecEncodeResult.size());
                // serialize perform an additional copy
                for (const auto& blockView: ecEncodeResult) {
                    blocks.push_back(std::make_unique<ContextDataBlock>(blockView));
                }
                mt = pmt::MerkleTree::New(pmtConfig, blocks, _wp);
                // we no longer need the block view after tree generation.
                return true;
            }

            // Invoke for generation
            // Concurrent support
            // serializeFragments is called after initWithMessage
            // not include end: [start, start+1, ..., end-1]
            // This func will throw runtime error
            [[nodiscard]] std::string serializeFragments(int start, int end) const {
                const auto& proofs = mt->getProofs();
                if (start<0 || start>=end || end>fragmentCnt) {
                    throw std::out_of_range("index out of range");    // out of range
                }
                DCHECK(fragmentCnt == (int)proofs.size());
                auto flRet = encodeResultHolder->getAll();
                if (!flRet) {
                    throw std::logic_error("encodeResultHolder have not encode yet");
                }
                auto actualFragmentList = std::move(*flRet);
                // create and reserve data
                std::string data;
                const int depth = (int)proofs[start].Siblings.size();  // proofs[start] must exist
                auto reserveSize = sizeof(int)+(end-start)*(depth*pmt::defaultHashLen+sizeof(uint32_t));  // additional 64B
                for (auto i=start; i<end; i++) {
                    reserveSize += actualFragmentList[i].size()+sizeof(int)*10;
                }

                data.reserve(reserveSize);
                // create serializer
                auto out = zpp::bits::out(data);
                // 1. write depth(compressed)
                out((int64_t) depth).or_throw();

                for (auto i=start; i<end; i++) {
                    const pmt::Proof& currentProof = proofs[i];
                    auto lastIndex = depth-1; // start copy from index
                    if (i != start) {
                        // reduce serialize size
                        const pmt::Proof& previousProof = proofs[i-1];
                        DCHECK(currentProof.Siblings.size() == previousProof.Siblings.size());
                        while (*previousProof.Siblings[lastIndex] == *currentProof.Siblings[lastIndex]) {
                            lastIndex--;
                            if (lastIndex < 0) {
                                break;
                            }
                        }
                    }
                    // 2. write path
                    out((uint32_t) currentProof.Path, (int64_t) lastIndex+1).or_throw();   // record the size, not the index
                    // 3. write proof vector
                    for (auto j=0; j<lastIndex+1; j++) {
                        const pmt::hashString& sj = *currentProof.Siblings[j];
                        out(sj).or_throw();
                    }
                    // 4. write actual data
                    out(actualFragmentList[i]).or_throw();
                }
                DCHECK(data.size() <= reserveSize) << "please reserve data size, actual size: " << data.size() << " estimate size " << reserveSize;
                return data;
            }
            [[nodiscard]] inline const auto& getRoot() const { return mt->getRoot(); }

            friend class BlockFragmentGenerator;

        protected:
            explicit Context(const Config& ecConfig, util::thread_pool_light* wp)
                : _ecConfig(ecConfig), fragmentCnt(_ecConfig.dataShardCnt+_ecConfig.parityShardCnt), _wp(wp),
                proofCacheGuard(fragmentCnt), proofCache(fragmentCnt), regenerateProofs(fragmentCnt), ecDecodeResult(fragmentCnt) { }

        private:
            std::unique_ptr<util::ErasureCode> ec;
            // store all encode result
            std::unique_ptr<util::EncodeResult> encodeResultHolder;
            Config _ecConfig;
            const int fragmentCnt;
            pmt::Config pmtConfig;
            util::thread_pool_light* _wp;
            std::unique_ptr<pmt::MerkleTree> mt = nullptr;
            // for verification only
            std::vector<std::atomic<bool>> proofCacheGuard;
            std::vector<std::vector<pmt::hashString>> proofCache;
            std::vector<pmt::Proof> regenerateProofs;
            std::vector<std::string> ecDecodeResult;
            // store all decode result
            std::unique_ptr<util::DecodeResult> decodeResultHolder;
        };

    public:
        template<class ErasureCodeType=util::GoErasureCode>
        requires std::is_base_of<util::ErasureCode, ErasureCodeType>::value
        BlockFragmentGenerator(const std::vector<Config>& cfgList, int poolCnt, int threadCount=0) {
            size_t max_x = 0, max_y = 0;
            for (const auto& cfg: cfgList) {
                if ((size_t)cfg.dataShardCnt > max_x) {
                    max_x = (size_t)cfg.dataShardCnt;
                }
                if ((size_t)cfg.parityShardCnt > max_y) {
                    max_y = (size_t)cfg.parityShardCnt;
                }
            }
            ecMap.resize(max_x);
            semaMap.resize(max_x);
            for (auto i=0; i<(int)max_x; i++) {
                ecMap[i].resize(max_y);
                semaMap[i] = std::vector<moodycamel::LightweightSemaphore>(max_y);
            }

            for (const auto& cfg: cfgList) {
                // index=actual-1
                auto x = (size_t)cfg.dataShardCnt - 1;
                auto y = (size_t)cfg.parityShardCnt - 1;
                // check if we have already allocate
                if (ecMap[x][y] != nullptr) {
                    continue;
                }
                // allocate new queue
                auto queue = std::make_unique<ECListType>(poolCnt);
                auto& sema = semaMap[x][y];
                for (int i=0; i<poolCnt; i++) {
                    queue->push(std::make_unique<ErasureCodeType>(cfg.dataShardCnt, cfg.parityShardCnt));
                    sema.signal();
                }
                ecMap[x][y] = std::move(queue);
            }
            wp = std::make_unique<util::thread_pool_light>(threadCount);   // size: thread cnt
        }
        
        ~BlockFragmentGenerator() = default;

        BlockFragmentGenerator(const BlockFragmentGenerator&) = delete;

        std::unique_ptr<Context> getEmptyContext(const Config& cfg) {
            auto x = (size_t)cfg.dataShardCnt - 1;
            auto y = (size_t)cfg.parityShardCnt - 1;
            if (ecMap.size() < x+1 ||ecMap[x].size() < y+1) {
                LOG(ERROR) << "ecMap index out of range.";
                return nullptr;
            }
            util::wait_for_sema(semaMap[x][y]); // acquire 1
            std::unique_ptr<Context> context(new Context(cfg, wp.get()));
            ecMap[x][y]->pop(context->ec);
            return context;
        }

        bool freeContext(std::unique_ptr<Context> context) {
            auto x = (size_t)context->_ecConfig.dataShardCnt - 1;
            auto y = (size_t)context->_ecConfig.parityShardCnt - 1;
            ecMap[x][y]->push(std::move(context->ec));
            semaMap[x][y].signal();
            return true;
        }

        [[nodiscard]] auto* getThreadPoolPtr() const {
            return wp.get();
        }

    private:
        using ECListType = rigtorp::MPMCQueue<std::unique_ptr<util::ErasureCode>>;
        std::vector<std::vector<std::unique_ptr<ECListType>>> ecMap;
        std::vector<std::vector<moodycamel::LightweightSemaphore>> semaMap;
        std::unique_ptr<util::thread_pool_light> wp;
    };
}