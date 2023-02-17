//
// Created by peng on 11/29/22.
//

#pragma once

#include "common/erasure_code.h"
#include "common/parallel_merkle_tree.h"
#include "common/thread_pool_light.h"
#include "common/matrix_2d.h"
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
            // must be equal for both validator and generator
            int instanceCount = 1;
            // count of concurrent generator+validator
            int concurrency = 1;

            [[nodiscard]] bool valid() const { return dataShardCnt>0 && parityShardCnt>=0 && instanceCount>0 && concurrency>0;}
        };

        class Context {
            class ContextDataBlock: public pmt::DataBlock {
            public:
                explicit ContextDataBlock(pmt::ByteString dataView) : _dataView(dataView) {}

                [[nodiscard]] pmt::ByteString Serialize() const override {
                    return _dataView;
                }
            private:
                // this is not the actual data!
                pmt::ByteString _dataView;
            };

        public:
            ~Context() = default;
            Context(const Context&) = delete;

            // check the config of the current instance
            [[nodiscard]] const Config& getConfig() const {
                return _ecConfig;
            }

            // Invoke for validation
            // support concurrent validation
            // This func will throw runtime error
            // WARNING: THE CALLER MUST KEEP THE RAW FRAGMENT UNTIL GENERATED, std::string_view raw
            bool validateAndDeserializeFragments(const pmt::HashString& root, std::string_view raw, int start, int end) {
                if (start<0 || start>=end || end>fragmentCnt) {
                    LOG(ERROR) << "index out of range!";
                    return false;
                }
                auto in = zpp::bits::in(raw);
                // 1. read depth(compressed)
                int64_t depth = 0;
                in(depth).or_throw();

                pmt::Proof* previousProofPtr = nullptr;
                // When currentDS.cacheGuard is true, we need to get the omitted proof
                std::vector<pmt::HashString> cacheWhenGuard(depth);
                // must read cacheWhenGuard instead of previousProofPtr
                bool cacheWhenGuardFlag = false;

                for (auto i=start; i<end; i++) {
                    auto& currentDS = decodeStorageList[i];
                    // Prevent concurrent modify of the same fragment
                    bool falseFlag = false;
                    if (!currentDS.cacheGuard.compare_exchange_strong(falseFlag, true, std::memory_order_release, std::memory_order_relaxed)) {
                        if (previousProofPtr != nullptr) {
                            // copy ALL proofs
                            for (auto j = 0; j < (int)cacheWhenGuard.size(); j++) {
                                cacheWhenGuard[j] = *previousProofPtr->Siblings[j];
                            }
                        }
                        for (int offset=0; offset<_ecConfig.instanceCount; offset++) {
                            in.position() += sizeof(int); // skip Path
                            int64_t currentProofSize = 0;
                            in(currentProofSize).or_throw();
                            // in.position() += currentProofSize * sizeof(pmt::hashString);
                            //---We have to read proofs but cache them locally
                            // Update cacheWhenGuard when necessary
                            for (auto j = 0; j < currentProofSize; j++) {
                                in(cacheWhenGuard[j]).or_throw();
                            }
                            //---
                            std::string_view fragment;
                            in(fragment).or_throw();
                        }
                        cacheWhenGuardFlag = true;
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
                        if (cacheWhenGuardFlag) {
                            DCHECK(currentProofCache.empty());
                            currentProofCache.resize(depth);
                            // first copy new data Siblings.push_back
                            for (auto j = 0; j < currentProofSize; j++) {
                                in(currentProofCache[j]).or_throw();
                                currentProof.Siblings.push_back(&currentProofCache[j]);
                            }
                            // copy CacheWhenGuard into cache
                            for (auto j=currentProofSize; j<depth; j++) {
                                currentProofCache[j] = cacheWhenGuard[j];
                                currentProof.Siblings.push_back(&currentProofCache[j]);
                            }
                            // cacheWhenGuard can be successfully deleted
                            cacheWhenGuardFlag = false;
                        } else {
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
                        }
                        previousProofPtr = &currentProof;
                        // 4. read actual data
                        std::string_view fragment;
                        in(fragment).or_throw();
                        ContextDataBlock fragmentView(fragment);
                        // we have finished loading all proofs, verify them.
                        // This function is Already Called by worker thread, no need to parallel verify
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
            // NOTE: bufferOut size is AT LEAST larger than the actualMessageSize
            bool regenerateMessage(int actualMessageSize, std::string& bufferOut) {
                // Calculate thr fragment len deterministically, using the actualMessageSize and instanceCount
                const auto fragmentLen = actualMessageSize % _ecConfig.instanceCount ? actualMessageSize / _ecConfig.instanceCount + 1: actualMessageSize / _ecConfig.instanceCount;
                std::vector<std::vector<std::string_view>> svListPartialView(_ecConfig.instanceCount);
                for(int i=0; i<_ecConfig.instanceCount; i++) {    // svListPartialView.size()
                    svListPartialView[i].resize(decodeStorageList.size());
                    for(auto j=0; j<fragmentCnt; j++) {   // decodeStorageList.size()
                        if ((int)decodeStorageList[j].encodeFragment.size() > i) {
                            svListPartialView[i][j] = decodeStorageList[j].encodeFragment[i];
                        }
                    }
                }
                // init a string container with a fixed size
                if ((int)bufferOut.size() < actualMessageSize) {
                    bufferOut.resize(actualMessageSize);
                }
                // the actual data size is different for the last fragment
                auto dataSize=fragmentLen;
                std::vector<std::future<bool>> futureList(_ecConfig.instanceCount);
                for(auto i=0; i<_ecConfig.instanceCount; i++) {     // (int)svListPartialView.size()
                    if (i == _ecConfig.instanceCount - 1) {
                        dataSize=(int)bufferOut.size() - i*fragmentLen;
                    }
                    futureList[i] = wpForMTAndEC->submit([&, dataSize=dataSize, idx=i]() {
                        // Error handling
                        if (!ec[idx]->decodeWithBuffer(svListPartialView[idx], dataSize, bufferOut.data()+idx*fragmentLen, dataSize)) {
                            util::OpenSSLSHA256 hash;
                            for (const auto &pv: svListPartialView) {
                                hash.update(pv.data(), pv.size());
                            }
                            LOG(ERROR) << "Regenerate message failed, hash: "
                                       << util::OpenSSLSHA256::toString(*hash.final());
                            return false;
                        }
                        return true;
                    });
                }
                for (auto& future: futureList) {
                    if (!future.get()) {
                        LOG(ERROR) << "Reconstruct message error!";
                        return false;
                    }
                }
                return true;
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
                    futureList[i] = wpForMTAndEC->submit([this, idx=i, &message, &len]()->bool {
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
                // leaf size is too big (all leaves have the equal size)
                if (ecEncodeResult[0].size() > 1024) {
                    pmtConfig.LeafGenParallel = true;
                }
                // too many leaves
                if (ecEncodeResult.size() > 1024) {
                    pmtConfig.RunInParallel = true;
                }
                // _ecConfig.instanceCount indicate the number of parallel running instance
                pmtConfig.NumRoutines = (int)wpForMTAndEC->get_thread_count() / _ecConfig.instanceCount;
                std::vector<std::unique_ptr<pmt::DataBlock>> blocks;
                blocks.reserve(ecEncodeResult.size());
                // serialize perform an additional copy
                for (const auto& blockView: ecEncodeResult) {
                    blocks.push_back(std::make_unique<ContextDataBlock>(blockView));
                }
                mt = pmt::MerkleTree::New(pmtConfig, blocks, wpForMTAndEC);
                // we no longer need the block view after tree generation.
                return true;
            }

            // Invoke for generation
            // Concurrent support
            // serializeFragments is called after initWithMessage
            // not include end: [start, start+1, ..., end-1]
            // This func will throw runtime error
            [[nodiscard]] bool serializeFragments(int start, int end, std::string& bufferOut) const {
                const auto& proofs = mt->getProofs();
                if (start<0 || start>=end || end>fragmentCnt) {
                    LOG(ERROR) << "index out of range!";
                    return false;
                }
                DCHECK(fragmentCnt*_ecConfig.instanceCount == (int)proofs.size());
                if (ecEncodeResult.empty()) {
                    LOG(ERROR) << "encodeResultHolder have not encode yet";
                    return false;
                }

                // create and reserve data
                const int depth = (int)proofs[start].Siblings.size();  // proofs[start] must exist
                auto reserveSize = sizeof(int)+(end-start)*_ecConfig.instanceCount*(depth*pmt::defaultHashLen+sizeof(uint32_t));
                // i: fragment id. if 3 instance, fragment#1[0, 1, 2] fragment#2[3, 4, 5]
                for (auto i=start*_ecConfig.instanceCount; i<end*_ecConfig.instanceCount; i++) {
                    reserveSize += ecEncodeResult[i].size()+sizeof(int)*10;
                }
                if (bufferOut.size() < reserveSize) {
                    bufferOut.resize(reserveSize);
                }

                // create serializer
                auto out = zpp::bits::out(bufferOut);
                out.reset(0);
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
                        const pmt::HashString& sj = *currentProof.Siblings[j];
                        out(sj).or_throw();
                    }
                    // 4. write actual data
                    out(ecEncodeResult[i]).or_throw();
                }
                DCHECK(bufferOut.size() <= reserveSize) << "please reserve data size, actual size: " << bufferOut.size() << " estimate size " << reserveSize;
                bufferOut.resize(out.position());
                return true;
            }

            [[nodiscard]] inline const auto& getRoot() const { return mt->getRoot(); }

            [[nodiscard]] inline auto getDataShardCnt() const { return _ecConfig.dataShardCnt; }

            friend class BlockFragmentGenerator;

        protected:
            explicit Context(const Config& ecConfig, util::thread_pool_light* wpForMTAndEC_)
                    : _ecConfig(ecConfig), fragmentCnt(_ecConfig.dataShardCnt+_ecConfig.parityShardCnt),
                      encodeResultHolder(_ecConfig.instanceCount), decodeResultHolder(_ecConfig.instanceCount),
                      wpForMTAndEC(wpForMTAndEC_), decodeStorageList(fragmentCnt) { }

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
            util::thread_pool_light* wpForMTAndEC;
            // when encoding, set the mt to keep all the proofs data
            std::unique_ptr<pmt::MerkleTree> mt = nullptr;
            // store all decode result
            struct DecodeStorage {
                std::atomic<bool> cacheGuard;
                // cache the string to avoid nullptr error
                // pmt::Proof regenerateProofs will use the string_view of it
                std::vector<pmt::HashString> cache;
                // the proofs load from cache
                std::vector<pmt::Proof> mtProofs;
                // the piece of fragment view load from cache
                std::vector<std::string_view> encodeFragment;
            };
            std::vector<DecodeStorage> decodeStorageList;
        };

    public:
        // wpForMTAndEC_ is shared within ALL BlockFragmentGenerator (if not singleton)
        // wpForMTAndEC_ is used to:
        // 1. generate merkle tree parallel in serialize phase.
        // 2. encode and decode parallel in both phase.
        template<class ErasureCodeType=util::GoErasureCode>
        requires std::is_base_of<util::ErasureCode, ErasureCodeType>::value
        BlockFragmentGenerator(const std::vector<Config>& cfgList, util::thread_pool_light* wpForMTAndEC_)
                : wpForMTAndEC(wpForMTAndEC_) {
            int max_x = 0, max_y = 0;
            for (const auto& cfg: cfgList) {
                if (cfg.dataShardCnt > max_x) {
                    max_x = cfg.dataShardCnt;
                }
                if (cfg.parityShardCnt > max_y) {
                    max_y = cfg.parityShardCnt;
                }
            }
            ecMap.reset(max_x, max_y);
            semaMap.reset(max_x, max_y);

            for (const auto& cfg: cfgList) {
                // index=actual-1
                auto x = cfg.dataShardCnt - 1;
                auto y = cfg.parityShardCnt - 1;
                // check if we have already allocate
                if (ecMap(x, y) != nullptr) {
                    continue;
                }
                auto totalInstanceCount = cfg.instanceCount*cfg.concurrency;
                // allocate new queue
                auto queue = std::make_unique<ECListType>(totalInstanceCount);
                auto& sema = semaMap(x, y);
                // fill in ec instance
                for (int i=0; i<totalInstanceCount; i++) {
                    queue->push(std::make_unique<ErasureCodeType>(cfg.dataShardCnt, cfg.parityShardCnt));
                }
                ecMap(x, y) = std::move(queue);
                sema.signal(totalInstanceCount);
            }
        }

        ~BlockFragmentGenerator() = default;

        BlockFragmentGenerator(const BlockFragmentGenerator&) = delete;

        // The caller must ensure the total acquire instance is smaller than the remain instance!
        // Or there may be a deadlock!
        std::shared_ptr<Context> getEmptyContext(const Config& cfg) {
            auto x = cfg.dataShardCnt - 1;
            auto y = cfg.parityShardCnt - 1;
            if (ecMap.x() < x+1 ||ecMap.y() < y+1) {
                LOG(ERROR) << "ecMap index out of range.";
                return nullptr;
            }
            util::wait_for_sema(semaMap(x, y), cfg.instanceCount);
            std::shared_ptr<Context> context(new Context(cfg, wpForMTAndEC), [this](Context* ret){
                this->freeContext(std::unique_ptr<Context>(ret));
            });
            context->ec.resize(cfg.instanceCount);
            for(int i=0; i<cfg.instanceCount; i++) {
                ecMap(x, y)->pop(context->ec[i]);
            }
            return context;
        }

    protected:
        bool freeContext(std::unique_ptr<Context> context) {
            auto x = context->_ecConfig.dataShardCnt - 1;
            auto y = context->_ecConfig.parityShardCnt - 1;
            // restore all ec
            for(int i=0; i<context->_ecConfig.instanceCount; i++) {
                ecMap(x, y)->push(std::move(context->ec[i]));
            }
            semaMap(x, y).signal(context->_ecConfig.instanceCount);
            return true;
        }

    private:
        using ECListType = rigtorp::MPMCQueue<std::unique_ptr<util::ErasureCode>>;
        util::Matrix2D<std::unique_ptr<ECListType>> ecMap;
        util::Matrix2D<moodycamel::LightweightSemaphore> semaMap;
        util::thread_pool_light* wpForMTAndEC;
    };
}