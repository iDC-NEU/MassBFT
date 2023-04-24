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

#include "glog/logging.h"

namespace peer::consensus {
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

        bool pushBlockWithOrder(int subChainId, int blockNumber, std::unordered_map<int, int> vectorClock) {
            std::unique_lock guard(mutex);
            auto* cell = findCell(subChainId, blockNumber, true);
            if (cell == nullptr) {  // both find and create failed
                return false;
            }
            // reserve size
            if (cell->decisions.empty()) {
                cell->decisions.reserve(subChainCount);
            }
            cell->decisions.push_back(std::move(vectorClock));
            if ((int)cell->decisions.size() == subChainCount) {
                return decide(cell);
            }
            return true;
        }

    public:
        struct Cell {
            int subChainId = -1;
            int blockNumber = -1;
            std::vector<std::unordered_map<int, int>> decisions;
            // after we collect ALL decisions, we generate the final vector clock
            std::unordered_map<int, int> finishedVectorClock;
            // after we collect ALL decisions, we calculate depends and depended
            std::unordered_map<int, int> depends;
            std::unordered_map<int, std::unordered_set<int>> depended;

            // after depends is empty, finished=true, and clear depended recursively.
            inline bool resolvedDependencies() const {
                return depends.empty() && !finishedVectorClock.empty();
            }

            inline bool calculatedFinalVC() const {
                return !finishedVectorClock.empty();
            }
        };

        struct Chain {
            int lastFinished = -1;
            std::unordered_map<int, std::unique_ptr<Cell>> blocks;
        };

        void setDeliverCallback(auto&& cb) { callback = std::forward<decltype(cb)>(cb); }

    protected:
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
                    return cell;
                }
                return nullptr;
            }
            return block->second.get();
        }

        // after we get all decisions, we create a final decision
        bool decide(Cell* cell) {
            cell->finishedVectorClock = MergeVectorClock(cell->decisions);
            if (cell->finishedVectorClock.empty()) {
                return false;   // vector clock merged error
            }
            LOG(INFO) << cell->subChainId << " " << cell->blockNumber << ", weight:" << cell->finishedVectorClock[0]
                      << " " << cell->finishedVectorClock[1] << " " << cell->finishedVectorClock[2];
            // calculate depends list
            for (const auto& it: cell->finishedVectorClock) {
                if (it.second == -1) {  // in it.first sub chain, current cell depend on nothing
                    continue;
                }
                // find the cell it depends on, create if not exist due to network delay
                auto rhs = findCell(it.first, it.second, true);
                //----CHECK CELL START----
                CHECK(rhs != nullptr) << "Input error, indicating a bug!";
                if (rhs->subChainId == cell->subChainId) {
                    CHECK(rhs->blockNumber == cell->blockNumber - 1) << "Block number check failed, indicating a bug.";
                }
                //----CHECK CELL END----
                if (rhs->resolvedDependencies()) {
                    continue;   // rhs is finished
                }
                // rhs < cell and rhs is not finished, add reference to waiting list
                rhs->depended[cell->subChainId].insert(cell->blockNumber);
                cell->depends[rhs->subChainId] = rhs->blockNumber;
            }
            // if depends is empty, wake up all cells in depended list
            return wakeup(cell);
        }

        // return false when error
        bool wakeup(Cell* cell) {
            std::stack<Cell*> stack;
            stack.push(cell);
            while (!stack.empty()) {
                auto* ret = stack.top();
                stack.pop();
                if (!wakeupInStack(ret, stack)) {
                    return false;
                }
            }
            return true;
        }

        // only called by wakeup
        inline bool wakeupInStack(Cell* cell, std::stack<Cell*>& stack) {
            if (!cell->resolvedDependencies()) {
                return true;    // not all depends block are solved
            }
            if (cell->blockNumber <= chains.at(cell->subChainId).lastFinished) {
                return true;    // repeated calls
            }
            // We are able to determine the order of the block among the sub chains
            for (const auto& chainId: cell->depended) {
                for (const auto& blockNumber: chainId.second) {
                    auto* ret = findCell(chainId.first, blockNumber, false);
                    if (ret == nullptr) {
                        LOG(ERROR) << "Can not find cell in wakeup func, should not happen!";
                        return false;
                    }
                    // CHECK IN CASE
                    auto it = ret->depends.find(cell->subChainId);
                    if (it == ret->depends.end()) {
                        CHECK(false) << "depends inconsistent";
                    }
                    CHECK(it->second == cell->blockNumber) << "block number inconsistent";
                    ret->depends.erase(it); // erase the doubly linked list
                    stack.push(ret);
                }
            }
            cell->depended.clear();
            CHECK(chains.at(cell->subChainId).lastFinished == cell->blockNumber - 1);
            chains.at(cell->subChainId).lastFinished = cell->blockNumber;
            // cell is finished, call the callback function
            if (callback != nullptr) {
                callback(cell);
            }
            return true;
        }

    public:
        static std::unordered_map<int, int> MergeVectorClock(const std::vector<std::unordered_map<int, int>>& clocks) {
            if (clocks.empty()) {
                return {};
            }
            std::unordered_map<int, int> res;
            // init res based on clocks[0]
            for (const auto& it: clocks[0]) {
                res[it.first] = -1;
            }
            // start merging
            for (const auto& clock: clocks) {
                for (const auto& it: clock) {
                    auto ret = res.find(it.first);
                    if (ret == res.end()) { // if we can not find the key in res
                        return {};  // input contains error
                    }
                    ret->second = std::max(ret->second, it.second);
                }
            }
            return res;
        }

        // return true when lhs < rhs;
        static inline bool DeterministicCompareCells(const Cell* lhs, const Cell* rhs) {
            // compare vector clock
            for (const auto& it: lhs->finishedVectorClock) {
                auto ret = rhs->finishedVectorClock.find(it.first);
                CHECK(ret != rhs->finishedVectorClock.end()) << "vector clocks contains error!";
                if (it.second == ret->second) {
                    continue;
                }
                return it.second < ret->second;
            }
            // when two cells have the same vector clock
            if (lhs->subChainId < rhs->subChainId) {
                return true;
            }
            if (lhs->subChainId > rhs->subChainId) {
                return false;
            }
            // have equal subChainId
            CHECK(lhs->blockNumber != rhs->blockNumber) << "two block number are equal";
            return lhs->blockNumber < rhs->blockNumber;
        }

    private:
        int subChainCount = 0;
        std::mutex mutex;
        std::unordered_map<int, Chain> chains;
        std::function<void(const Cell*)> callback;
    };

}