//
// Created by peng on 2/16/23.
//

#pragma once

#include "peer/replicator/block_fragment_generator.h"

#include "common/cv_wrapper.h"
#include "bthread/countdown_event.h"

#include <random>

namespace tests {
    class BFGUtils {
    public:
        BFGUtils() {
            // crypto and message pre-allocate
            util::OpenSSLSHA256::initCrypto();
            FillDummy(message, 1024*1024*2);
            tp = std::make_unique<util::thread_pool_light>();
        }

        // return region id
        int addCFG(int dataShardCnt = 4, int parityShardCnt = 4, int instanceCount = 1, int concurrency = 1) {
            cfgList.push_back({dataShardCnt, parityShardCnt, instanceCount, concurrency});
            CHECK(cfgList.back().valid());
            return (int)cfgList.size()-1;
        }

        void startBFG() {
            bfg = std::make_shared<peer::BlockFragmentGenerator>(cfgList, tp.get());
        }

        std::shared_ptr<peer::BlockFragmentGenerator::Context> getContext(int regionId) {
            auto ret = bfg->getEmptyContext(cfgList[regionId]);
            ret->initWithMessage(message);
            return ret;
        };

        std::string generateMockFragment(peer::BlockFragmentGenerator::Context* context, proto::BlockNumber number, uint32_t start, uint32_t end) const {
            proto::EncodeBlockFragment fragment{{}, number, {}, {}, start, end, {}};
            // fill the rest fields
            auto encodeMessageBuf = FillFragment(context, start, end);
            fragment.size = message.size();
            fragment.encodeMessage = std::move(encodeMessageBuf);
            fragment.root = context->getRoot();
            // serialize to string
            std::string dataOut;
            if(!fragment.serializeToString(&dataOut, 0, true)) {
                CHECK(false) << "Encode message fragment failed!";
            }
            return dataOut;
        }

        static std::string FillFragment(peer::BlockFragmentGenerator::Context* context, uint32_t start, uint32_t end) {
            std::string buffer;
            CHECK(context->serializeFragments(start, end, buffer)) << "create fragment failed!";
            return buffer;
        }

        static void FillDummy(std::string& dummyBytes, int len) {
            constexpr static const char alphabet[] =
                    "abcdefghijklmnopqrstuvwxyz"
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    "0123456789";
            std::random_device rd;
            std::default_random_engine rng(rd());
            std::uniform_int_distribution<> dist(0, sizeof(alphabet) / sizeof(*alphabet) - 2);

            dummyBytes.clear();
            dummyBytes.reserve(len);
            std::generate_n(std::back_inserter(dummyBytes), len, [&]() {
                return alphabet[dist(rng)];
            });
        }


    public:
        std::string message;
        std::unique_ptr<util::thread_pool_light> tp;
        std::vector<peer::BlockFragmentGenerator::Config> cfgList;
        std::shared_ptr<peer::BlockFragmentGenerator> bfg;
    };
}
