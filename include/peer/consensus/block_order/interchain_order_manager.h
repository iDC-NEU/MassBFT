//
// Created by user on 23-4-5.
//

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>

#include "glog/logging.h"

namespace peer::consensus::v2 {
    // manage states from a set of correlated sub chains
    class InterChainOrderManager {
    public:
        void setSubChainCount(int count) {
            std::unique_lock guard(mutex);
            subChainCount = count;
            for (int i=0; i<count; i++) {
                chains[i] = {};
            }
        }

        struct Cell {
            int subChainId = -1;
            int blockNumber = -1;
            std::unordered_map<int, int> decisions;
            // after we collect ALL decisions, we generate the final vector clock
            // prevent std::numeric_limits<int>::max() increase overflow
            std::vector<int64_t> finalDecision;

            // if final vc is calculated, the block can commit
            inline bool canAddToCommitBuffer() const {
                return !finalDecision.empty();
            }

            bool mergeDecisions() {
                finalDecision = std::vector<int64_t>(decisions.size(), -1);
                for (int i=0; i<(int)finalDecision.size(); i++) {
                    auto ret = decisions.find(i);
                    CHECK(ret != decisions.end()) << "Missing decisions!";
                    finalDecision[i] = ret->second;
                }
                // additional check, may be unnecessary
                CHECK(decisions[subChainId] == blockNumber - 1 || decisions[subChainId] == std::numeric_limits<int>::max()) << "Input error!";
                return true;
            }

            void printDebugString() const {
                std::string weightStr = "{";
                for (const auto& it: this->finalDecision) {
                    weightStr.append(std::to_string(it)).append(", ");
                }
                weightStr.append("}");
                LOG(INFO) << this->subChainId << " " << this->blockNumber << ", weight:" << weightStr;
            }

            bool operator<(const Cell* rhs) const {
                CHECK(this->canAddToCommitBuffer() && rhs->canAddToCommitBuffer());
                // compare vector clock (happen before)
                for (int i=0; i<(int)finalDecision.size(); i++) {
                    auto& l = finalDecision[i];
                    auto& r = rhs->finalDecision[i];
                    if (l == r) {
                        continue;
                    }
                    return l < r;
                }
                // when two cells have the same vector clock
                if (subChainId < rhs->subChainId) {
                    return true;
                }
                if (subChainId > rhs->subChainId) {
                    return false;
                }
                // when two cells have the same subChainId
                CHECK(blockNumber != rhs->blockNumber) << "two block number are equal";
                return blockNumber < rhs->blockNumber;
            }

        };

    protected:
        class CommitBuffer {
        public:
            void push(Cell* c) {
                buffer.push_back(c);
                int rhsPos = (int)buffer.size()-1;
                for (int i = (int)buffer.size()-2; i >= 0; i--) {
                    if (!(*buffer[rhsPos] < buffer[i])) {
                        break;
                    }
                    std::swap(buffer[rhsPos], buffer[i]);
                    rhsPos = i;
                }
            }

            Cell* pop() {
                if (empty()) {
                    return nullptr;
                }
                auto* elem = buffer.front();
                buffer.erase(buffer.begin());
                return elem;
            }

            [[nodiscard]] Cell* top() const {
                if (empty()) {
                    return nullptr;
                }
                return buffer.front();
            }

            [[nodiscard]] bool empty() const {
                return buffer.empty();
            }

            void print() const {
                Cell* prev = nullptr;
                for (const auto& it: buffer) {
                    it->printDebugString();
                    if (prev) {
                        CHECK(*prev < it);
                    }
                    prev = it;
                }
            }

        private:
            std::vector<Cell*> buffer;
        };

    protected:
        struct Chain {
            int lastFinished = -1;  // The vector clocks before last finished have been added to commit buffer
            std::unordered_map<int, std::unique_ptr<Cell>> blocks;
        };

        Cell* findCell(int subChainId, int blockNumber, bool createIfNotExist) {
            auto chain = chains.find(subChainId);
            if (chain == chains.end()) {
                return nullptr;
            }
            auto& blocks = chain->second.blocks;
            auto block = blocks.find(blockNumber);
            if (block == blocks.end()) {
                if (createIfNotExist) {
                    // create if not exist
                    auto ret = blocks.emplace(blockNumber, std::make_unique<Cell>());
                    if (!ret.second) {
                        return nullptr;
                    }
                    auto* cell = ret.first->second.get();
                    cell->subChainId = subChainId;
                    cell->blockNumber = blockNumber;
                    cell->decisions.reserve(subChainCount);
                    // fill in crash group decisions
                    for (const auto& it: invalidGroupList) {
                        // maybe unnecessary
                        cell->decisions.insert(std::make_pair(it, std::numeric_limits<int>::max()));
                    }
                    return cell;
                }
                return nullptr;
            }
            return block->second.get();
        }

    public:
        bool invalidateChain(int subChainId) {
            std::unique_lock guard(mutex);
            if (!invalidGroupList.insert(subChainId).second) {
                return true;    // already invalidated
            }
            LOG(WARNING) << "Invalidate chain of group: " << subChainId;
            // all unfilled decisions is marked invalid
            for (int i=0; i<subChainCount; i++) {
                auto chain = chains.find(i);
                if (chain == chains.end()) {
                    return false;
                }
                for (int j=chain->second.lastFinished; ; j++) {
                    auto* cell = findCell(i, j, false); // do not create if not exist
                    if (cell == nullptr) {
                        break;  // the sub chain is purged
                    }
                    if (cell->decisions.contains(subChainId)) {
                        continue;   // has already decide
                    }
                    if (!pushDecisionWithoutLock(i, j, std::make_pair(subChainId, std::numeric_limits<int>::max()))) {
                        return false;   // decide it
                    }
                }
            }
            return true;
        }

    private:
        std::unordered_set<int> invalidGroupList;

    public:
        inline bool pushDecision(int subChainId, int blockNumber, std::pair<int, int> decision) {
            std::unique_lock guard(mutex);
            return pushDecisionWithoutLock(subChainId, blockNumber, decision);
        }

    protected:
        bool pushDecisionWithoutLock(int subChainId, int blockNumber, std::pair<int, int> decision) {
            auto* cell = findCell(subChainId, blockNumber, true);
            CHECK(cell != nullptr) << "Impl error!";
            // prevent unordered concurrent access
            if (cell->canAddToCommitBuffer()) {
                // return true if not an internal error (should not be displayed)
                return true;
            }
            // LOG(INFO) << "Add decision for cell: " << subChainId << ", blk: " << blockNumber << ", dec: " << decision.first << ", " << decision.second;
            cell->decisions.insert(std::move(decision));
            // prevent out-of-order insert
            while (cell != nullptr && cell->blockNumber-1 == chains[cell->subChainId].lastFinished) {
                // enter decision loop
                if ((int)cell->decisions.size() != subChainCount) {
                    DCHECK((int)cell->decisions.size() < subChainCount);
                    return true;
                }
                if (!cell->mergeDecisions()) {
                    return false;
                }
                if (!decideV2(cell)) {
                    return false;
                }
                cell = findCell(cell->subChainId, cell->blockNumber+1, false);
            }
            return true;
        }

    protected:
        // after we get all decisions, we create a final decision
        bool decideV2(Cell* cell) {
            // The vc of all blocks that the block depends on has been calculated,
            // it is added to the commit collection to determine its position later
            CHECK(chains[cell->subChainId].lastFinished == cell->blockNumber - 1);
            chains[cell->subChainId].lastFinished = cell->blockNumber;
            // DLOG(INFO) << "Push block:"; cell->printDebugString();
            commitBuffer.push(cell);

            while(!commitBuffer.empty()) {
                cell = commitBuffer.top();
                // check if all cell it depends on is committed already
                for (int i=0; i<(int)cell->finalDecision.size(); i++) {
                    // iter through chain, i: chainId
                    if (i == cell->subChainId) {
                        continue;   // skip itself
                    }
                    if (cell->finalDecision[i] == std::numeric_limits<int>::max()) {
                        continue;   // skip crash decisions
                    }
                    const auto& lastFinished = chains[i].lastFinished;
                    if (lastFinished == -1) {
                        // Optimization
                        auto rhs = createBarCell(i);
                        if (!cell->operator<(rhs.get())) {
                            return true; // retry later
                        }
                        continue;   // can safely commit
                    }
                    // Normal cases
                    if (lastFinished < cell->finalDecision[i]) {
                        return true; // retry later
                    }
                    // ----compare vector clocks start
                    auto* rhs = findCell(i, lastFinished, false);
                    CHECK(rhs != nullptr) << "Impl error!";
                    // ----DEFAULT VERSION START
                    // auto cmpResult = cell->operator<(rhs);
                    // ----DEFAULT VERSION END
                    // Note: explain finalDecision[rhs->subChainId]++, compare with chain[id][lastFinish+1]
                    // ----Optimization 3 lines, TODO: double check
                     rhs->finalDecision[rhs->subChainId]++;
                     auto cmpResult = cell->operator<(rhs);
                     rhs->finalDecision[rhs->subChainId]--;
                    // ----Optimization END
                    if (!cmpResult) {
                        // the block after res still may < than cell, still have to wait
                        return true;
                    }
                    // the block after res still must larger than cell, can add to buffer safely
                    // ----compare vector clocks end
                }
                commitBuffer.pop();
                callback(cell);
            }
            return true;
        }

    public:
        void setDeliverCallback(auto&& cb) { callback = std::forward<decltype(cb)>(cb); }

        void printBuffer() const { commitBuffer.print(); }

    protected:
        [[nodiscard]] std::unique_ptr<Cell> createBarCell(int subChainId) const {
            auto barCell = std::make_unique<Cell>();
            // the vc of next block must be greater(or equal) than barCell
            barCell->finalDecision = std::vector<int64_t>(subChainCount, -1);
            barCell->subChainId = subChainId;
            barCell->blockNumber = -1;
            return barCell;
        }

    private:
        std::mutex mutex;
        std::unordered_map<int, Chain> chains;
        int subChainCount = 0;
        CommitBuffer commitBuffer;
        std::function<void(const Cell*)> callback;
    };

    class OrderAssigner {
    public:
        void setLocalChainId(int chainId) {
            std::unique_lock guard(mutex);
            _chainId = chainId;
        }

        // return -1, -1 when order is already assigned
        [[nodiscard]] std::pair<int, int> getBlockOrder(int chainId, int blockId) {
            DCHECK(_chainId >= 0) << "have not inited yet!";
            std::unique_lock guard(mutex);
            auto& res = _blockVotes.getRef(chainId, blockId);
            DCHECK(res.blockId == blockId);
            if (res.finished) {   // already voted
                return std::make_pair(-1, -1);
            }
            res.finished = true;
            auto ret = std::make_pair(_chainId, _myClock);
            if (_chainId == chainId) {
                ret = std::make_pair(_chainId, blockId - 1);
            }
            return ret;
        }

        // return -1 when is ok.
        [[nodiscard]] int addVoteForBlock(int chainId, int blockId, int voteChainId) {
            if (_chainId == chainId) {
                return -1;  // skip local chain
            }
            std::unique_lock guard(mutex);
            auto& res = _blockVotes.getRef(chainId, blockId);
            if (res.finished) {
                return -1;
            }
            res.votes.insert(voteChainId);
            return (int)res.votes.size();
        }

        // return -1 when is ok.
        [[nodiscard]] int getVoteForBlock(int chainId, int blockId) {
            if (_chainId == chainId) {
                return -1;  // skip local chain
            }
            std::unique_lock guard(mutex);
            auto res = _blockVotes.getRef(chainId, blockId);
            if (res.finished) {
                return -1;
            }
            return (int)res.votes.size();
        }

        bool increaseLocalClock(int chainId, int blockId) {
            if (_chainId == chainId) {
                std::unique_lock guard(mutex);
                if (blockId - 1 < _myClock) {
                    return false;    //  stale call
                }
                // LOG(INFO) << "Leader " << chainId << " increase clock to " << blockId;
                CHECK(blockId - 1 == _myClock) << "Consensus local block out of order!";
                _myClock = blockId;
                return true;
            }
            return false;   // receive other regions block
        }

    protected:
        class BlockVotes {
        public:
            struct Slot {
                int blockId{};
                std::unordered_set<int> votes;
                bool finished{};
            };

            BlockVotes() {
                for (auto& it: _blockVotesCount) {
                    it.resize(MAX_BLOCK_QUEUE_SIZE);
                }
            }

            [[nodiscard]] Slot& getRef(int chainId, int blockId) {
                auto& res = _blockVotesCount[chainId][blockId % MAX_BLOCK_QUEUE_SIZE];
                DCHECK(res.blockId <= blockId) << "Stale get";
                if (res.blockId < blockId) {    // clear it
                    res.blockId = blockId;
                    res.votes.clear();
                    res.finished = false;
                }
                return res;
            }

        private:
            constexpr static const auto MAX_GROUP_SIZE = 100;

            constexpr static const auto MAX_BLOCK_QUEUE_SIZE = 256;

            std::vector<Slot> _blockVotesCount[MAX_GROUP_SIZE];
        };

    private:
        mutable std::mutex mutex;
        int _myClock = -1;
        int _chainId = -1;
        BlockVotes _blockVotes;
    };

}