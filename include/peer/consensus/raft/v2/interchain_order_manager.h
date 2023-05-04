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
#include <optional>

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
            std::unordered_map<int, std::unordered_set<int>> depended;

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
                    weightStr.append(std::to_string(it.second) + ": " + std::to_string(it.second)).append(", ");
                }
                weightStr.append("}");
                LOG(INFO) << this->subChainId << " " << this->blockNumber << ", weight:" << weightStr;
            }

            bool operator<(const Cell* rhs) const {
                DCHECK(this->canAddToCommitBuffer() && rhs->canAddToCommitBuffer());
                // compare vector clock (happen before)
                std::optional<bool> result = std::nullopt;
                for (const auto& it: finalDecision) {
                    auto ret = rhs->finalDecision.find(it.first);
                    CHECK(ret != rhs->finalDecision.end()) << "vector clocks contains error!";
                    if (it.second == ret->second) {
                        continue;
                    }
                    if (it.second < ret->second) {
                        if (result != std::nullopt && result == false) {
                            result = std::nullopt;
                            break;
                        }
                        *result = true;  // happen before
                    }
                    if (it.second < ret->second) {
                        if (result != std::nullopt && result == true) {
                            result = std::nullopt;
                            break;
                        }
                        *result = false; // happen after
                    }
                }
                if (result != std::nullopt) {
                    return *result;
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

            // minq3:  0 1 2 3 4 5 6 7 8 9
            // bool operator() (const int l, const int r) const { return l > r; }
            bool operator() (const Cell* a, const Cell* b) { return !a->operator<(b); }
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
            // cell->printDebugString();
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
                    res->depended[cell->subChainId].insert(cell->blockNumber);
                    cell->dependsCount++;
                }
            }
            if (cell->canAddToCommitBuffer()) {
                // The vc of all blocks that the block depends on has been calculated,
                // it is added to the commit collection to determine its position later
                CHECK(chains[cell->subChainId].lastFinished == cell->blockNumber - 1);
                chains[cell->subChainId].lastFinished++;
                commitBuffer.push(cell);
            }
            // remove all blocks that depends on it
            for (const auto& chainId: cell->depended) {
                for (const auto& blockNumber: chainId.second) {
                    auto* res = findCell(chainId.first, blockNumber, false);
                    CHECK(res != nullptr) << "Impl error!";
                    res->dependsCount--;
                    if (res->canAddToCommitBuffer()) {
                        // add to commit set
                        CHECK(chains[res->subChainId].lastFinished == res->blockNumber - 1);
                        chains[res->subChainId].lastFinished++;
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
                    if (chains[it.first].lastFinished < it.second) {
                        return; // retry later
                    }
                }
                commitBuffer.pop();
                callback(cell);
            }
        }

    private:
        std::mutex mutex;
        std::unordered_map<int, Chain> chains;
        int subChainCount = 0;
        std::priority_queue<Cell*, std::vector<Cell*>, Cell> commitBuffer;
        std::function<void(const Cell*)> callback;
    };

    class OrderAssigner {
    public:
        void setSubChainIds(const std::vector<int>& chainIds) {
            std::unique_lock guard(mutex);
            subChainCount = (int)chainIds.size();
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
        int subChainCount = 0;
        std::unordered_map<int, int> currentDecision;
    };

}