//
// Created by user on 23-4-5.
//

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>

#include "glog/logging.h"

namespace peer::consensus::v2 {
    class InterChainOrderManager {
    public:
        static constexpr int INVALID_WATERMARK = -1;
        static constexpr int RESERVE_BLOCK_COUNT = 1024*1024;

        void setGroupCount(int count) {
            std::unique_lock guard(mutex);
            chains = std::vector<Chain>(count);
            for (auto& it: chains) {
                it.chain.reserve(RESERVE_BLOCK_COUNT);
            }
            // init heads
            heads = CommitBuffer();
            for (int i=0; i<count; i++) {
                auto* cell = createBlockIfNotExist(i, 0);
                heads.push(cell); // the group ID of cell is the index of heads
            }
        }

        void setDeliverCallback(auto&& cb) { deliverCallback = std::forward<decltype(cb)>(cb); }

    private:
        class CommitBuffer;

    public:
        class Cell {
        public:
            explicit Cell(int groupCount, int groupId_, int blockId_)
                    : groupId(groupId_),
                      blockId(blockId_),
                      watermarks(groupCount, INVALID_WATERMARK),
                      isSet(groupCount, false) {
                setIthWaterMark(groupId_, blockId_); // this is known by default
            }

            bool mustLessThan(const Cell* rhs) const {
                if (this->groupId == rhs->groupId && this->blockId == rhs->blockId) {
                    CHECK(this == rhs);
                    return true;    // we do not compare the same entry
                }
                for (int i=0; i<(int)this->watermarks.size(); i++) {
                    if (!this->isSet[i]) {
                        return false;
                    }
                    if (this->watermarks[i] != rhs->watermarks[i]) {
                        return this->watermarks[i] < rhs->watermarks[i];
                    }
                }
                if (this->blockId != rhs->blockId) {
                    return this->blockId < rhs->blockId;
                }
                return this->groupId < rhs->groupId;
            }

            bool setIthWaterMark(int i, int value) {
                if (isSet[i]) {
                    // this may fail due to multiple call of invalidate signal (which does not matter correctness)
                    // CHECK(watermarks[i] == value) << "voteGroup: " << i << ", value: " << value << ", original: " << watermarks[i];
                    return false;
                }
                CHECK(watermarks[i] <= value);  // ensure compare fairness
                isSet[i] = true;
                watermarks[i] = value;
                return true;
            }

            void printDebugString() const {
                std::string weightStr = "{";
                for (int i=0; i<(int)watermarks.size(); i++) {
                    weightStr.append(std::to_string(watermarks[i]) + " " + std::to_string(isSet[i])).append(", ");
                }
                weightStr.append("}");
                LOG(INFO) << this->groupId << " " << this->blockId << ", watermarks:" << weightStr;
            }

            const int groupId;

            const int blockId;

        private:
            friend class CommitBuffer;

            std::vector<int> watermarks;

            std::vector<bool> isSet;
        };

    private:
        struct Chain {
            int height = -1;    // only for debug!
            std::vector<std::unique_ptr<Cell>> chain;
        };

        class CommitBuffer {
        public:
            void push(Cell* c) {    // call only when init
                CHECK(c->groupId == (int)buffer.size());
                buffer.push_back(c);
            }

            Cell* globalMinimum() {
                CHECK(!buffer.empty());
                for (auto left : buffer) {
                    bool canPop = true;
                    for (auto & right : buffer) {
                        if (!left->mustLessThan(right)) {
                            canPop = false;
                            break;
                        }
                    }
                    if (canPop) {   // left is global minimum
                        return left;
                    }
                }
                return nullptr;
            }

            void exchange(Cell* prev, Cell* next) {
                CHECK(buffer[prev->groupId] == prev);
                CHECK(prev->blockId == next->blockId - 1);
                CHECK(prev->groupId == next->groupId);
                CHECK(prev->watermarks.size() == next->watermarks.size());
                buffer[next->groupId] = next;
                // update estimate watermark of next
                for (int i=0; i<(int)prev->watermarks.size(); i++) {
                    if (next->isSet[i]) {
                        CHECK(prev->watermarks[i] <= next->watermarks[i]);
                        continue;
                    }
                    next->watermarks[i] = prev->watermarks[i];
                }
                // set bits of invalidated group
                for (auto& it: invalidateMap) {
                    if (it.second.isInvalid == false) {
                        continue;
                    }
                    // LOG(INFO) << c->groupId << ", " << c->blockId;
                    // LOG(INFO) << c->watermarks[it.first] << ", " << it.second.height;
                    // CHECK(c->watermarks[it.first] <= it.second.height);
                    next->watermarks[it.first] = prev->watermarks[it.first];
                    next->isSet[it.first] = true;
                }
            }

            void print() const {
                for (const auto& it: buffer) {
                    it->printDebugString();
                }
            }

            void updateEstimate(int groupId, int watermark) {
                for (auto& it: buffer) {
                    if (groupId == it->groupId) {
                        continue; // skip updating local group (its watermark has been pre-set)
                    }
                    if (it->isSet[groupId]) {
                        continue;
                    }
                     CHECK(it->watermarks[groupId] <= watermark) << "watermarks:" << it->watermarks[groupId];
                    it->watermarks[groupId] = watermark;
                }
            }

            void invalidateChain(int groupId, int height) {
                if (invalidateMap[groupId].isInvalid) {
                    return; // already invalidated
                }
                LOG(WARNING) << "Invalidate chain of group: " << groupId << ", height: " << height;
                invalidateMap[groupId].isInvalid = true;

                for (auto& it: buffer) {
                    CHECK(it->watermarks[groupId] <= height);
                    // if (it->isSet[groupId] == true) {
                    //     continue;
                    // }
                    // it->watermarks[groupId] = height;
                    it->isSet[groupId] = true;
                }
            }

        private:
            // buffer[i] is the next unordered block proposed by the ith group.
            std::vector<Cell*> buffer;

            struct InvalidateSlot {
                bool isInvalid = false;
            };

            std::unordered_map<int, InvalidateSlot> invalidateMap;
        };

    protected:
        Cell* createBlockIfNotExist(int groupId, int blockId) {
            CHECK((int)chains.size() > groupId);
            auto& blocks = chains[groupId];
            // expand blocks if too small
            while ((int)blocks.chain.size() <= blockId) {
                blocks.chain.push_back(std::make_unique<Cell>(static_cast<int>(chains.size()), groupId, blockId));    // insert new cell
            }
            return blocks.chain[blockId].get();
        }

        void popBlocks() {
            while (auto *prev = heads.globalMinimum()) {
                auto *next = createBlockIfNotExist(prev->groupId, prev->blockId + 1);
                // exchange element
                heads.exchange(prev, next);
                deliverCallback(prev);
            }   // return if prev == nullptr
        }

    public:
        void pushDecision(int groupId, int blockId, int voteGroupId, int voteGroupWatermark) {
            if (voteGroupId == groupId) {
                LOG_IF(WARNING, voteGroupWatermark != blockId) << "voteGroupWatermark is not equal to blockId!";
                voteGroupWatermark = blockId;
                if (chains.size() > 1) {
                    return;    // we will learn voteGroupWatermark from other groups later
                }
            }
            std::unique_lock guard(mutex);
            auto* cell = createBlockIfNotExist(groupId, blockId);
            if (!cell->setIthWaterMark(voteGroupId, voteGroupWatermark)) {
                return; // Need to remove duplicates
            }

            // use to test if all nodes runs in the same order
            // static auto gid = std::this_thread::get_id();
            // LOG(INFO) << "Node " << gid << " execute " << groupId << ", " << blockId << ", " << voteGroupId << ", " <<voteGroupWatermark;
            // static int counter = 0;
            // if (counter++ % 200 == 0) {
            //     heads.print();
            // }

            // update chain height (for debug)
            chains[voteGroupId].height = std::max(chains[voteGroupId].height, voteGroupWatermark);
            chains[groupId].height = std::max(chains[groupId].height, blockId);

            // voteGroupId cannot vote watermark smaller than voteGroupWatermark after this!
            heads.updateEstimate(voteGroupId, voteGroupWatermark);
            // resort the queue in updateEstimateWatermark
            popBlocks();
        }

        void invalidateChain(int groupId) {
            std::unique_lock guard(mutex);
            heads.invalidateChain(groupId, chains[groupId].height);
            popBlocks();  // maybe there are new elements that can pop
        }

    private:
        std::mutex mutex;   // for chains

        std::vector<Chain> chains;  // store all the blocks in order

        std::function<void(const peer::consensus::v2::InterChainOrderManager::Cell* cell)> deliverCallback;  // execute block in order

        CommitBuffer heads;
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
                ret = std::make_pair(_chainId, blockId);
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
                std::set<int> votes;
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