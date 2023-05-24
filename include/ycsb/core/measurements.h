//
// Created by user on 23-5-24.
//

#pragma once

#include <string>
#include "proto/block.h"
#include "common/timer.h"
#include "common/phmap.h"

namespace ycsb::core {
    class Measurements {
    public:
        void beginTransaction(const std::string& digest, uint64_t ts) {
            map[digest] = ts;
        }

        std::vector<uint64_t> onReceiveBlock(const proto::Block& block) {
            auto now = util::Timer::time_now_ns();
            std::vector<uint64_t> spanList;
            spanList.reserve(block.body.userRequests.size());
            for (auto& it: block.body.userRequests) {
                auto& digest = it->getSignature().digest;
                map.erase_if(std::string_view(reinterpret_cast<const char *>(digest.data()), digest.size()), [&](auto& v) {
                    spanList.push_back(now - v.second);
                    return true;
                });
            }
            return spanList;
        }

        [[nodiscard]] size_t getPendingTransactionCount() const { return map.size(); }

    private:
        util::MyFlatHashMap<std::string, uint64_t, std::mutex> map;
    };
}