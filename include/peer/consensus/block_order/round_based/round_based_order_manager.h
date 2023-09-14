//
// Created by user on 23-9-14.
//

#include <mutex>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <functional>

#include "glog/logging.h"

namespace peer::consensus::rb {
    class RoundBasedOrderManager {
    public:
        void setSubChainCount(int count) {
            std::unique_lock guard(mutex);
            subChainCount = count;
        }

        bool invalidateChain(int subChainId) {
            std::unique_lock guard(mutex);
            if (subChainCount <= subChainId) {
                return false;   // overflow
            }
            if (!invalidGroupList.insert(subChainId).second) {
                return true;    // already invalidated
            }
            LOG(WARNING) << "Invalidate chain of group: " << subChainId;
            tryCommitBlock();
            return true;
        }

        // block finished global consensus
        inline bool pushDecision(int subChainId, int blockNumber) {
            std::unique_lock guard(mutex);
            return pushDecisionWithoutLock(subChainId, blockNumber);
        }

        void setDeliverCallback(auto&& cb) { callback = std::forward<decltype(cb)>(cb); }

    protected:
        // blockNumber: round i of consensus
        bool pushDecisionWithoutLock(int subChainId, int blockNumber) {
            if (subChainCount <= subChainId) {
                return false;   // overflow
            }
            // create if not exist
            if (!receiveMap.contains(blockNumber)) {
                receiveMap[blockNumber].resize(subChainCount);
            }
            receiveMap[blockNumber][subChainId] = true; // received the block
            tryCommitBlock();
            return true;
        }


        void tryCommitBlock() {
            while (isRoundCommitted(lastCommittedRound + 1)) {
                // DLOG(INFO) << "Start commit round: " << lastCommittedRound;
                for (int i=0; i<subChainCount; i++) {
                    if (invalidGroupList.contains(i)) {
                        continue;
                    }
                    callback(i, lastCommittedRound);
                }
            }
        }

        // inside lock
        bool isRoundCommitted(int round) {
            CHECK(round >= 0);
            if (round <= lastCommittedRound) {
                return true;
            }
            for (int i = lastCommittedRound+1; i <= round; i++) {
                auto it = receiveMap.find(i);
                if (it == receiveMap.end()) {
                    return false;   // can not find the round (maybe not generated)
                }
                for (const auto& idx: it->second) {
                    if (idx == false && !invalidGroupList.contains(idx)) {
                        return false;   // the group is not crash and I have not received the block
                    }
                }
                // I have received all block for round i
                lastCommittedRound += 1;
                // recycle memory
                receiveMap.erase(it);
            }
            return true;
        }

    private:
        std::mutex mutex;
        int subChainCount = -1;
        int lastCommittedRound = -1;
        std::unordered_set<int> invalidGroupList;
        std::unordered_map<int, std::vector<bool>> receiveMap;
        std::function<void(int chainId, int blockId)> callback;
    };
}
