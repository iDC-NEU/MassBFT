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
        void setSubChainIds(const std::vector<int>& chainIds) {
            std::unique_lock guard(mutex);
            subChainCount = (int)chainIds.size();
            for (const auto& it: chainIds) {
                chains[it] = {};
            }
        }

        struct Cell {
            int subChainId = -1;
            int blockNumber = -1;
            std::vector<std::unordered_map<int, int>> decisions;
            // after we collect ALL decisions, we generate the final vector clock
            std::unordered_map<int, int> finalDecision;
            // after we collect ALL decisions, we calculate depends and depended
            int dependsCount = 0;
            std::unordered_map<int, std::priority_queue<int, std::vector<int>, std::greater<>>> depended;

            // if all vc in block it relied on is calculated, the block can commit
            inline bool canAddToCommitBuffer() const {
                return mergedFinalDecision() && dependsCount == 0;
            }

            inline bool mergedFinalDecision() const {
                return !finalDecision.empty();
            }

            bool mergeDecisions() {
                if (decisions.empty()) {
                    return false;
                }
                // init res based on clocks[0]
                for (const auto& it: decisions[0]) {
                    finalDecision[it.first] = -1;
                }
                // start merging
                for (const auto& clock: decisions) {
                    for (const auto& it: clock) {
                        auto ret = finalDecision.find(it.first);
                        if (ret == finalDecision.end()) { // if we can not find the key in res
                            return {};  // input contains error
                        }
                        ret->second = std::max(ret->second, it.second);
                    }
                }
                return true;
            }

            void printDebugString() const {
                std::string weightStr = "{";
                for (const auto& it: this->finalDecision) {
                    weightStr.append(std::to_string(it.first) + ": " + std::to_string(it.second)).append(", ");
                }
                weightStr.append("}");
                LOG(INFO) << this->subChainId << " " << this->blockNumber << ", weight:" << weightStr;
            }

            bool operator<(const Cell* rhs) const {
                CHECK(this->canAddToCommitBuffer() && rhs->canAddToCommitBuffer());
                // compare vector clock (happen before)
                bool before = false;
                bool after = false;
                for (const auto& it: finalDecision) {
                    auto ret = rhs->finalDecision.find(it.first);
                    CHECK(ret != rhs->finalDecision.end()) << "vector clocks contains error!";
                    if (it.second == ret->second) {
                        continue;
                    }
                    if (it.second < ret->second) {
                        before = true;
                    }
                    if (it.second > ret->second) {
                        after = true;
                    }
                }
                if (before && !after) {
                    return true;
                }
                if (!before && after) {
                    return false;
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

            bool operator>(const Cell* rhs) const { return !(*this < rhs); }

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
                    return cell;
                }
                return nullptr;
            }
            return block->second.get();
        }

    public:
        bool pushDecision(int subChainId, int blockNumber, std::unordered_map<int, int> decision) {
            std::unique_lock guard(mutex);
            auto* cell = findCell(subChainId, blockNumber, true);
            if (cell == nullptr) {  // both find and create failed
                return false;
            }
            cell->decisions.push_back(std::move(decision));
            // enter decision loop
            if ((int)cell->decisions.size() == subChainCount) {
                if (!cell->mergeDecisions()) {
                    return false;
                }
                return decide(cell);
            }
            return true;
        }

    protected:
        // after we get all decisions, we create a final decision
        bool decide(Cell* cell) {
            // calculate the blocks that it depends on
            for (const auto& it: cell->finalDecision) {  // iter through chain
                auto startWith = it.second; // reverse check dep block
                if (startWith == -1) {  // in this sub chain, current cell depend on nothing
                    continue;
                }
                // the block depends on all blocks smaller than its vc
                for (int i=startWith; i>=0; i--) {
                    // may depend on (chainId = it.first, blockId = i)
                    auto* res = findCell(it.first, i, true);
                    CHECK(res != nullptr) << "Impl error!";
                    if (res->mergedFinalDecision()) {
                        // The final vc of all blocks before ret in this sub chain has also been calculated,
                        // so it is safe to return
                        break;
                    }
                    // add to dep list
                    res->depended[cell->subChainId].push(cell->blockNumber);
                    cell->dependsCount++;
                }
            }
            if (cell->canAddToCommitBuffer()) {
                // The vc of all blocks that the block depends on has been calculated,
                // it is added to the commit collection to determine its position later
                CHECK(chains[cell->subChainId].lastFinished == cell->blockNumber - 1);
                chains[cell->subChainId].lastFinished = cell->blockNumber;
                // DLOG(INFO) << "Push block:"; cell->printDebugString();
                commitBuffer.push(cell);
            }
            // remove all blocks that depends on it
            for (auto& chainId: cell->depended) {
                int blockNumber = -1;
                while (!chainId.second.empty()) {
                    if (blockNumber >= chainId.second.top()) {
                        chainId.second.pop();
                        continue;
                    }   // remove redundant value
                    blockNumber = chainId.second.top();
                    chainId.second.pop();
                    auto* res = findCell(chainId.first, blockNumber, false);
                    CHECK(res != nullptr) << "Impl error!";
                    res->dependsCount--;
                    CHECK(res->dependsCount >= 0);
                    if (res->canAddToCommitBuffer()) {
                        // add to commit set, lastFinished++
                        auto& tmp = chains[res->subChainId].lastFinished;
                        CHECK(tmp == res->blockNumber - 1);
                        tmp = res->blockNumber;
                        // DLOG(INFO) << "Push block:"; res->printDebugString();
                        commitBuffer.push(res);
                    }
                }
            }
            cell->depended.clear();
            commitCells();
            return true;
        }

    public:
        void setDeliverCallback(auto&& cb) { callback = std::forward<decltype(cb)>(cb); }

    protected:
        void commitCells() {
            while(!commitBuffer.empty()) {
                auto *cell = commitBuffer.top();
                // check if all cell it depends on is committed already
                for (const auto &it: cell->finalDecision) {  // iter through chain
                    if (it.first == cell->subChainId) {
                        continue;   // skip itself
                    }
                    const auto& lastFinished = chains[it.first].lastFinished;
                    if (lastFinished == -1) {
                        continue;
                    }
                    if (lastFinished < it.second) {
                        return; // retry later
                    }
                    // ----compare vector clocks start
                    auto* res = findCell(it.first, lastFinished, false);
                    CHECK(res != nullptr) << "Impl error!";
                    // Optimization, TODO: double check
                    res->finalDecision[res->subChainId]++;
                    auto cmpResult = cell->operator<(res);
                    res->finalDecision[res->subChainId]--;
                    if (!cmpResult) {
                        // the block after res still may < than cell, still have to wait
                        return;
                    }
                    // the block after res still must larger than cell, can add to buffer safely
                    // ----compare vector clocks end
                }
                // commitBuffer.print();
                commitBuffer.pop();
                callback(cell);
            }
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
        void setSubChainIds(const std::vector<int>& chainIds) {
            std::unique_lock guard(mutex);
            for (const auto& it: chainIds) {
                currentDecision[it] = -1;
            }
        }

        std::unordered_map<int, int> getBlockOrder(int chainId, int blockId) {
            std::unique_lock guard(mutex);
            auto ret = currentDecision;
            auto it = currentDecision.find(chainId);
            CHECK(it != currentDecision.end() && it->second == blockId - 1);
            it->second = blockId;
            return ret;
        }

    private:
        std::mutex mutex;
        std::unordered_map<int, int> currentDecision;
    };

}