//
// Created by user on 23-5-24.
//

#pragma once

#include "proto/block.h"
#include "common/timer.h"
#include "common/phmap.h"

namespace ycsb::core {
    class Measurements {
    public:
        void beginTransaction(const std::string& digest, uint64_t ts) {
            map[digest] = ts;
        }

        // If latency not found, it is set to 0.
        std::vector<uint64_t> getTxnLatency(const proto::Block& block) {
            auto now = util::Timer::time_now_ns();
            std::vector<uint64_t> spanList;
            spanList.reserve(block.body.userRequests.size());
            for (auto& it: block.body.userRequests) {
                auto& digest = it->getSignature().digest;
                auto ret = map.erase_if(std::string_view(reinterpret_cast<const char *>(digest.data()), digest.size()), [&](auto& v) {
                    spanList.push_back(now - v.second);
                    return true;
                });
                if (!ret) {
                    spanList.push_back(0);
                }
            }
            CHECK(block.body.userRequests.size() == spanList.size());
            return spanList;
        }

        [[nodiscard]] size_t getPendingTransactionCount() const { return map.size(); }

    private:
        util::MyFlatHashMap<std::string, uint64_t, std::mutex> map;
    };
}