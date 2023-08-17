//
// Created by peng on 11/29/22.
//

#pragma once

#include "common/erasure_code.h"
#include "common/parallel_merkle_tree.h"
#include "common/thread_pool_light.h"
#include "common/matrix_2d.h"

#include "rigtorp/MPMCQueue.h"
#include "glog/logging.h"

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

                [[nodiscard]] std::optional<pmt::HashString> Digest() const override {
                    return util::OpenSSLSHA256::generateDigest(_dataView.data(), _dataView.size());
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
            [[nodiscard]] bool validateAndDeserializeFragments(const pmt::HashString& root, std::string_view raw, int start, int end);

            // Invoke for validation
            // when enough pieces is found, we can regenerate message
            // No concurrent support, the caller must ensure ALL validateAndDeserializeFragments call is finished.
            // WARNING: EC hold the return value, it is not safe to use EC until you finished using the returned message
            // NOTE: bufferOut size is AT LEAST larger than the actualMessageSize
            [[nodiscard]] bool regenerateMessage(int actualMessageSize, std::string& bufferOut);

            // Invoke for generation
            // No concurrent support
            bool initWithMessage(std::string_view message);

            // Invoke for generation
            // Concurrent support
            // serializeFragments is called after initWithMessage
            // not include end: [start, start+1, ..., end-1]
            // This func will throw runtime error
            [[nodiscard]] bool serializeFragments(int start, int end, std::string& bufferOut, int offset=0) const;

            [[nodiscard]] inline const auto& getRoot() const { return mt->getRoot(); }

            [[nodiscard]] inline auto getDataShardCnt() const { return _ecConfig.dataShardCnt; }

            friend class BlockFragmentGenerator;

        protected:
            explicit Context(const Config& ecConfig, util::thread_pool_light* wpForMTAndEC_)
                    : _ecConfig(ecConfig), fragmentCnt(_ecConfig.dataShardCnt+_ecConfig.parityShardCnt),
                      encodeResultHolder(_ecConfig.instanceCount), decodeResultHolder(_ecConfig.instanceCount),
                      wpForMTAndEC(wpForMTAndEC_), decodeStorageList(fragmentCnt) { }

            template <typename F, typename... A>
            inline void push_task(F&& task, A&&... args) {
                if (_ecConfig.instanceCount > 1) {
                    wpForMTAndEC->push_emergency_task(std::forward<F>(task), std::forward<A>(args)...);
                } else {
                    task(std::forward<A>(args)...);
                }
            }
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
            CHECK(wpForMTAndEC != nullptr) << "Thread pool unset, can not start bfg";
            int max_x = 0, max_y = 0;
            for (const auto& cfg: cfgList) {
                CHECK(cfg.dataShardCnt > 0 && cfg.parityShardCnt >= 0) << "Shard input error!";
                if (cfg.dataShardCnt > max_x) {
                    max_x = cfg.dataShardCnt;
                }
                if (cfg.parityShardCnt > max_y) {
                    max_y = cfg.parityShardCnt;
                }
            }
            ecMap.reset(max_x + 1, max_y + 1);
            semaMap.reset(max_x + 1, max_y + 1);

            for (const auto& cfg: cfgList) {
                auto x = cfg.dataShardCnt;
                auto y = cfg.parityShardCnt;
                auto totalInstanceCount = cfg.instanceCount*cfg.concurrency;
                std::unique_ptr<ECListType> queue = nullptr;
                if (ecMap(x, y) != nullptr) {
                    // drain existing queue
                    auto oldQueue = std::move(ecMap(x, y));
                    queue = std::make_unique<ECListType>(totalInstanceCount + oldQueue->size());
                    while (!oldQueue->empty()) {
                        std::unique_ptr<util::ErasureCode> data;
                        oldQueue->pop(data);
                        DCHECK(data != nullptr);
                        queue->push(std::move(data));
                    }
                } else {
                    // allocate new queue
                    queue = std::make_unique<ECListType>(totalInstanceCount);
                }
                for (int i=0; i<totalInstanceCount; i++) {
                    queue->push(std::make_unique<ErasureCodeType>(cfg.dataShardCnt, cfg.parityShardCnt));
                }
                ecMap(x, y) = std::move(queue);
                auto& sema = semaMap(x, y);
                sema.signal(totalInstanceCount);
            }
        }

        virtual ~BlockFragmentGenerator() = default;

        BlockFragmentGenerator(const BlockFragmentGenerator&) = delete;

        // The caller must ensure the total acquire instance is smaller than the remain instance!
        // Or there may be a deadlock!
        [[nodiscard]] std::shared_ptr<Context> getEmptyContext(const Config& cfg);

    protected:
        bool freeContext(std::unique_ptr<Context> context);

    private:
        using ECListType = rigtorp::MPMCQueue<std::unique_ptr<util::ErasureCode>>;
        util::Matrix2D<std::unique_ptr<ECListType>> ecMap;
        util::Matrix2D<moodycamel::LightweightSemaphore> semaMap;
        util::thread_pool_light* wpForMTAndEC;
    };
}