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
            // the shard count for each instance
            int dataShardCnt = 1;
            int parityShardCnt = 0;
            // Instance for parallel processing
            int instanceCount = 1;
            int concurrency = 1;
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

            bool overrideECInstanceCount(int instanceCountOverride) {
                if (instanceCountOverride == 0 || instanceCountOverride > _ecConfig.instanceCount) {
                    return false;
                }
                _ecConfig.instanceCount = instanceCountOverride;
                return true;
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

                pmt::Proof* previousProofPtr = nullptr;
                for (auto i=start; i<end; i++) {
                    bool falseFlag = false;
                    auto& currentDS = decodeStorageList[i];
                    if (!currentDS.cacheGuard.compare_exchange_strong(falseFlag, true, std::memory_order_release, std::memory_order_relaxed)) {
                        CHECK(false);
                        in.position() += sizeof(int); // skip Path
                        int64_t currentProofSize = 0;
                        in(currentProofSize).or_throw();
                        in.position() += currentProofSize*sizeof(pmt::hashString);
                        std::string_view fragment;
                        in(fragment).or_throw();
                        continue;   // another thread is modifying this fragment
                    }
                    // resize to avoid invalid pointer
                    currentDS.mtProofs.resize(_ecConfig.instanceCount);
                    currentDS.encodeFragment.resize(_ecConfig.instanceCount);
                    auto &currentProofCache = currentDS.cache;
                    // reserve to avoid invalid pointer
                    currentProofCache.reserve(_ecConfig.instanceCount*depth);
                    for (int offset=0; offset<_ecConfig.instanceCount; offset++) {
                        auto &currentProof = currentDS.mtProofs[offset];

                        currentProof.Siblings.reserve(depth);
                        // 2. write path, compressed proof size
                        int64_t currentProofSize = 0;
                        in(currentProof.Path, currentProofSize).or_throw();
                        // 3. read proofs
                        auto originalSize = currentProofCache.size();
                        currentProofCache.resize(originalSize+currentProofSize);
                        for (auto j = originalSize; j < currentProofCache.size(); j++) {
                            in(currentProofCache[j]).or_throw();
                            currentProof.Siblings.push_back(&currentProofCache[j]);
                        }
                        if (previousProofPtr != nullptr) {
                            for (auto j = currentProofSize; j < depth; j++) {
                                currentProof.Siblings.push_back(previousProofPtr->Siblings[j]);
                            }
                        }
                        previousProofPtr = &currentProof;
                        // 4. read actual data
                        std::string_view fragment;
                        in(fragment).or_throw();
                        ContextDataBlock fragmentView(fragment);
                        // we have finished loading all proofs, verify them.
                        // TODO: parallel verify
                        auto verifyResult = pmt::MerkleTree::Verify(fragmentView, currentProof, root);
                        if (verifyResult && *verifyResult) {
                            currentDS.encodeFragment[offset] = fragment;
                        } else {
                            LOG(ERROR) << "Verification failed";
                            return false;
                        }
                    }
                }
                return true;
            }

            // Invoke for validation
            // when enough pieces is found, we can regenerate message
            // No concurrent support, the caller must ensure ALL validateAndDeserializeFragments call is finished.
            // WARNING: EC hold the return value, it is not safe to use EC until you finished using the returned message
            // NOTE: need to resize message to the actual size (if using the go erasure code)
            std::optional<std::string> regenerateMessage(size_t actualMessageSize) {
                auto fragmentLen = actualMessageSize % _ecConfig.instanceCount ? actualMessageSize / _ecConfig.instanceCount + 1: actualMessageSize / _ecConfig.instanceCount;
                std::vector<std::vector<std::string_view>> svListPartialView;
                svListPartialView.resize(_ecConfig.instanceCount);
                for(int i=0; i<(int)svListPartialView.size(); i++) {
                    svListPartialView[i].reserve(decodeStorageList.size());
                    for(const auto& sl:decodeStorageList) {
                        if ((int)sl.encodeFragment.size() > i) {
                            svListPartialView[i].push_back(sl.encodeFragment[i]);
                        } else {
                            svListPartialView[i].push_back(std::string_view());
                        }
                    }
                }
                for(int i=0; i<(int)svListPartialView.size(); i++) {
                    decodeResultHolder[i] = ec[i]->decode(svListPartialView[i]);
                    if (!decodeResultHolder[i]) {
                        util::OpenSSLSHA256 hash;
                        for (const auto &pv: svListPartialView) {
                            hash.update(pv.data(), pv.size());
                        }
                        LOG(ERROR) << "Regenerate message failed, hash: "
                                   << util::OpenSSLSHA256::toString(*hash.final());
                        return std::nullopt;
                    }
                }
                std::string ret;
                ret.reserve(fragmentLen*decodeResultHolder.size());
                for(const auto& drh: decodeResultHolder) {
                    ret.append(drh->getData()->substr(0, fragmentLen));
                }
                DCHECK(ret.size() < fragmentLen*decodeResultHolder.size()) << "want: " << fragmentLen*decodeResultHolder.size() << ", actual" << ret.size();
                ret.resize(actualMessageSize);
                return ret;
            }

            // Invoke for generation
            // No concurrent support
            bool initWithMessage(std::string_view message) {
                if (mt != nullptr) {
                    return false;   // already inited
                }
                auto actualMessageSize = message.size();
                std::vector<std::future<bool>> futureList(_ecConfig.instanceCount);
                // ceil may not perform correctly in some case
                auto len = actualMessageSize % _ecConfig.instanceCount ? actualMessageSize / _ecConfig.instanceCount + 1: actualMessageSize / _ecConfig.instanceCount;
                ecEncodeResult.resize(_ecConfig.instanceCount*fragmentCnt);
                for (int i=0; i<_ecConfig.instanceCount; i++) {
                    futureList[i] = _wp->submit([this, idx=i, &message, &len]()->bool {
                        // auto solve overflow
                        std::string_view msgView = message.substr(idx*len, (idx+1)*len);
                        auto encodeResult = ec[idx]->encode(msgView);
                        if (encodeResult == nullptr) {
                            return false;
                        }
                        auto ret = encodeResult->getAll();
                        if (!ret) {
                            return false;
                        }
                        DCHECK((int)ret->size() == fragmentCnt);
                        for(int j=0; j<fragmentCnt; j++) {
                            // index + offset(instance id)
                            ecEncodeResult[j*_ecConfig.instanceCount+idx] = (*ret)[j];
                        }
                        encodeResultHolder[idx] = std::move(encodeResult);
                        return true;
                    });
                }
                for (auto& ret: futureList) {
                    if (!ret.get()) {
                        LOG(ERROR) << "Encode failed";
                        return false;
                    }
                }
                pmt::Config pmtConfig;
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
            [[nodiscard]] std::string serializeFragments(int start, int end, size_t additionalReserveSize = 0) const {
                const auto& proofs = mt->getProofs();
                if (start<0 || start>=end || end>fragmentCnt) {
                    throw std::out_of_range("index out of range");    // out of range
                }
                DCHECK(fragmentCnt*_ecConfig.instanceCount == (int)proofs.size());
                if (ecEncodeResult.empty()) {
                    throw std::logic_error("encodeResultHolder have not encode yet");
                }
                // _ecConfig.instanceCount*fragmentCnt
                // create and reserve data
                std::string data;
                const int depth = (int)proofs[start].Siblings.size();  // proofs[start] must exist
                auto reserveSize = sizeof(int)+(end-start)*(depth*pmt::defaultHashLen+sizeof(uint32_t)) + additionalReserveSize;
                // i: fragment id. if 3 instance, fragment#1[0, 1, 2] fragment#2[3, 4, 5]
                for (auto i=start*_ecConfig.instanceCount; i<end*_ecConfig.instanceCount; i++) {
                    reserveSize += ecEncodeResult[i].size()+sizeof(int)*10;
                }

                data.reserve(reserveSize);
                // create serializer
                auto out = zpp::bits::out(data);
                // 1. write depth(compressed)
                out((int64_t) depth).or_throw();

                for (auto i=start*_ecConfig.instanceCount; i<end*_ecConfig.instanceCount; i++) {
                    const pmt::Proof& currentProof = proofs[i];
                    auto lastIndex = depth-1; // start copy from index
                    if (i != start*_ecConfig.instanceCount) {
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
                    out(ecEncodeResult[i]).or_throw();
                }
                DCHECK(data.size() <= reserveSize) << "please reserve data size, actual size: " << data.size() << " estimate size " << reserveSize;
                return data;
            }
            [[nodiscard]] inline const auto& getRoot() const { return mt->getRoot(); }

            friend class BlockFragmentGenerator;

        protected:
            explicit Context(const Config& ecConfig, util::thread_pool_light* wp)
                    : _ecConfig(ecConfig), fragmentCnt(_ecConfig.dataShardCnt+_ecConfig.parityShardCnt),
                      encodeResultHolder(_ecConfig.instanceCount), decodeResultHolder(_ecConfig.instanceCount),
                      _wp(wp), decodeStorageList(fragmentCnt) { }

        private:
            // config for util::ErasureCode ec
            Config _ecConfig;
            const int fragmentCnt;
            // ec is set by BlockFragmentGenerator
            std::vector<std::unique_ptr<util::ErasureCode>> ec;
            // Hold the most recent encode and decode result
            // avoid invalid memory pointer for string_view
            std::vector<std::unique_ptr<util::EncodeResult>> encodeResultHolder;
            std::vector<std::string_view> ecEncodeResult;   // hold all encode result
            std::vector<std::unique_ptr<util::DecodeResult>> decodeResultHolder;
            // the thread pool pointer shared by all instance
            util::thread_pool_light* _wp;
            // when encoding, set the mt to keep all the proofs data
            std::unique_ptr<pmt::MerkleTree> mt = nullptr;
            // store all decode result
            struct DecodeStorage {
                std::atomic<bool> cacheGuard;
                // cache the string to avoid nullptr error
                // pmt::Proof regenerateProofs will use the string_view of it
                std::vector<pmt::hashString> cache;
                // the proofs load from cache
                std::vector<pmt::Proof> mtProofs;
                // the piece of fragment view load from cache
                std::vector<std::string_view> encodeFragment;
            };
            std::vector<DecodeStorage> decodeStorageList;
        };

    public:
        template<class ErasureCodeType=util::GoErasureCode>
        requires std::is_base_of<util::ErasureCode, ErasureCodeType>::value
        BlockFragmentGenerator(const std::vector<Config>& cfgList, util::thread_pool_light* wp_) {
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
                auto totalInstanceCount = cfg.instanceCount*cfg.concurrency;
                // allocate new queue
                auto queue = std::make_unique<ECListType>(totalInstanceCount);
                auto& sema = semaMap[x][y];
                // fill in ec instance
                for (int i=0; i<totalInstanceCount; i++) {
                    queue->push(std::make_unique<ErasureCodeType>(cfg.dataShardCnt, cfg.parityShardCnt));
                }
                ecMap[x][y] = std::move(queue);
                sema.signal(totalInstanceCount);
            }
            wp = wp_;
        }
        
        ~BlockFragmentGenerator() = default;

        BlockFragmentGenerator(const BlockFragmentGenerator&) = delete;

        // The caller must ensure the total acquire instance is smaller than the remain instance!
        // Or there may be a deadlock!
        std::unique_ptr<Context> getEmptyContext(const Config& cfg) {
            auto x = (size_t)cfg.dataShardCnt - 1;
            auto y = (size_t)cfg.parityShardCnt - 1;
            if (ecMap.size() < x+1 ||ecMap[x].size() < y+1) {
                LOG(ERROR) << "ecMap index out of range.";
                return nullptr;
            }
            util::wait_for_sema(semaMap[x][y], cfg.instanceCount);
            std::unique_ptr<Context> context(new Context(cfg, wp));
            context->ec.resize(cfg.instanceCount);
            for(int i=0; i<cfg.instanceCount; i++) {
                ecMap[x][y]->pop(context->ec[i]);
            }
            return context;
        }

        bool freeContext(std::unique_ptr<Context> context) {
            auto x = (size_t)context->_ecConfig.dataShardCnt - 1;
            auto y = (size_t)context->_ecConfig.parityShardCnt - 1;
            // restore all ec
            for(int i=0; i<context->_ecConfig.instanceCount; i++) {
                ecMap[x][y]->push(std::move(context->ec[i]));
            }
            semaMap[x][y].signal(context->_ecConfig.instanceCount);
            return true;
        }

    private:
        using ECListType = rigtorp::MPMCQueue<std::unique_ptr<util::ErasureCode>>;
        std::vector<std::vector<std::unique_ptr<ECListType>>> ecMap;
        std::vector<std::vector<moodycamel::LightweightSemaphore>> semaMap;
        util::thread_pool_light* wp;
    };
}