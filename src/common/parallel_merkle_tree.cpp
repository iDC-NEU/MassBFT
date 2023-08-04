//
// Created by user on 23-7-4.
//

#include "common/parallel_merkle_tree.h"
#include "bthread/countdown_event.h"
#include "glog/logging.h"
#include <bitset>

namespace pmt {

    std::optional<HashString> Config::HashFunc(const HashString &h1, const HashString &h2) {
        util::OpenSSLSHA256 hash;
        if (!hash.update(h1.data(), h1.size()) ||
            !hash.update(h2.data(), h2.size()) ) {
            return std::nullopt;
        }
        return hash.final();
    }

    std::optional<HashString> Config::HashFunc(const ByteString &str) {
        util::OpenSSLSHA256 hash;
        if (!hash.update(str.data(), str.size())) {
            return std::nullopt;
        }
        return hash.final();
    }

    std::string Proof::toString() const {
        std::stringstream buf;
        buf << "Path: " << std::bitset<8>(this->Path) <<", Siblings: ";
        for(auto i=0; i<(int)Siblings.size(); i++) {
            buf << "\n\t" << "Siblings depth: "<< Siblings.size()-i << "\t" << util::OpenSSLSHA256::toString(*Siblings[i]);
        }
        return buf.str();
    }

    std::unique_ptr<MerkleTree> MerkleTree::New(const Config &c, const std::vector<std::unique_ptr<DataBlock>> &blocks, util::thread_pool_light *wpPtr) {
        if (blocks.size() <= 1) {
            LOG(ERROR) << "the number of data blocks must be greater than 1";
            return nullptr;
        }
        auto mt = std::unique_ptr<MerkleTree>(new MerkleTree(c));
        // task channel capacity is passed as 0, so use the default value: 2 * numWorkers
        if(wpPtr != nullptr) {
            mt->wp = wpPtr;
            if (mt->config.NumRoutines == 0) {
                mt->config.NumRoutines = (int)mt->wp->get_thread_count();
            }
        }
        // If NumRoutines is unset, then set NumRoutines to the thread pool count.
        mt->Depth = calTreeDepth((int) blocks.size());
        if (mt->config.RunInParallel || mt->config.LeafGenParallel) {
            if (!mt->leafGenParallel(blocks)) {
                LOG(ERROR) << "generate merkle tree failed";
                return nullptr;
            }
        } else {
            if (!mt->leafGen(blocks)) {
                LOG(ERROR) << "generate merkle tree failed";
                return nullptr;
            }
        }
        if (mt->config.Mode == ModeType::ModeProofGen) {
            if (mt->config.RunInParallel) {
                if (!mt->proofGenParallel()) {
                    LOG(ERROR) << "generate merkle proof failed";
                    return nullptr;
                }
            } else {
                if (!mt->proofGen()) {
                    LOG(ERROR) << "generate merkle proof failed";
                    return nullptr;
                }
            }
            return mt;
        }
        if (mt->config.Mode == ModeType::ModeTreeBuild || mt->config.Mode == ModeType::ModeProofGenAndTreeBuild) {
            if (!mt->treeBuild()) {
                LOG(ERROR) << "generate merkle proof failed";
                return nullptr;
            }
            if (mt->config.Mode == ModeType::ModeTreeBuild) {
                return mt;
            }
            // ModeProofGenAndTreeBuild
            mt->initProofs();
            if (mt->config.RunInParallel) {
                for (int i = 0; i < (int) mt->tree.size(); i++) {
                    mt->updateProofsParallel(mt->tree[i], (int) mt->tree[i].size(), i);
                }
            } else {
                for (int i = 0; i < (int) mt->tree.size(); i++) {
                    mt->updateProofs(mt->tree[i], (int) mt->tree[i].size(), i);
                }
            }
            return mt;
        }
        LOG(ERROR) << "invalid configuration mode";
        return nullptr;
    }

    uint32_t MerkleTree::calTreeDepth(int blockLen) {
        auto log2BlockLen = log2(double(blockLen));
        // check if log2BlockLen is an integer
        if (log2BlockLen != int(log2BlockLen)) {
            return uint32_t(log2BlockLen) + 1;
        }
        return uint32_t(log2BlockLen);
    }

    bool MerkleTree::proofGen() {
        const int numLeaves = (int) Leaves.size();
        initProofs();
        proofGenBufList.push_back(std::make_unique<std::vector<HashString>>(Leaves));
        std::vector<HashString>* buf = proofGenBufList.back().get();
        int prevLen = numLeaves;
        pmt::MerkleTree::fixOdd(*buf, prevLen);
        this->updateProofs(*buf, numLeaves, 0);

        for (auto step = 1; step < int(Depth); step++) {
            proofGenBufList.push_back(std::make_unique<std::vector<HashString>>(*buf));
            buf = proofGenBufList.back().get();  // must re-create buf, copy-on-write
            for (auto idx = 0; idx < prevLen; idx += 2) {
                auto res = Config::HashFunc((*buf)[idx], (*buf)[idx + 1]);
                if (!res) {
                    return false;
                }
                (*buf)[idx >> 1] = *res;
            }
            prevLen >>= 1;
            pmt::MerkleTree::fixOdd(*buf, prevLen);
            this->updateProofs(*buf, prevLen, step);
        }
        auto res = Config::HashFunc((*buf)[0], (*buf)[1]);
        if (!res) {
            return false;
        }
        Root = *res;
        return true;
    }

    void MerkleTree::fixOdd(std::vector<HashString> &buf, int &prevLen) {
        if ((prevLen & 1) == 0) {
            return;
        }
        HashString appendNode = buf[prevLen - 1];
        prevLen++;
        if ((int) buf.size() < prevLen) {
            buf.push_back(appendNode);
        } else {
            buf[prevLen - 1] = appendNode;
        }
    }

    void MerkleTree::updatePairProof(const std::vector<HashString> &buf, int idx, int batch, int step) {
        auto start = idx * batch;
        int end = start + batch < (int) Proofs.size() ? start + batch : (int) Proofs.size();
        for (auto i = start; i < end; i++) {
            Proofs[i].Path += 1 << step;
            Proofs[i].Siblings.push_back(&buf[idx + 1]);
        }
        start += batch;
        end = start + batch < (int) Proofs.size() ? start + batch : (int) Proofs.size();
        for (auto i = start; i < end; i++) {
            Proofs[i].Siblings.push_back(&buf[idx]);
        }
    }

    bool MerkleTree::leafGen(const std::vector<std::unique_ptr<DataBlock>> &blocks) {
        auto lenLeaves = blocks.size();
        this->Leaves.resize(lenLeaves);

        for (auto i = 0; i < (int) lenLeaves; i++) {
            auto hash = Config::HashFunc(blocks[i]->Serialize());
            if (!hash) {
                return false;
            }
            this->Leaves[i] = *hash;
        }
        return true;
    }

    bool MerkleTree::leafGenParallel(const std::vector<std::unique_ptr<DataBlock>> &blocks) {
        int lenLeaves = (int) blocks.size();
        this->Leaves.resize(lenLeaves);

        auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, lenLeaves);
        bthread::CountdownEvent countdown(numRoutines);
        bool ret = true;
        for (auto i = 0; i < numRoutines; i++) {
            push_task([&, start=i]{
                for (int j = start; j < lenLeaves; j += numRoutines) {
                    auto hash = Config::HashFunc(blocks[j]->Serialize());
                    if (!hash) {
                        ret = false;
                        break;  // error
                    }
                    this->Leaves[j] = *hash;
                }
                countdown.signal();
            });
        }
        countdown.wait();
        return ret;
    }

    bool MerkleTree::proofGenParallel() {
        this->initProofs();
        const int numLeaves = (int) Leaves.size();
        proofGenBufList.push_back(std::make_unique<std::vector<HashString>>(Leaves));
        std::vector<HashString>* buf1 = proofGenBufList.back().get();  // have to perform deep copy
        int prevLen = numLeaves;
        pmt::MerkleTree::fixOdd(*buf1, prevLen);
        this->updateProofsParallel(*buf1, numLeaves, 0);

        proofGenBufList.push_back(std::make_unique<std::vector<HashString>>(prevLen >> 1));
        std::vector<HashString>* buf2 = proofGenBufList.back().get();
        for (auto step = 1; step < int(Depth); step++) {
            auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, prevLen);
            bthread::CountdownEvent countdown(numRoutines);
            for (auto i = 0; i < numRoutines; i++) {
                push_task([&, start=i << 1] {
                    for (auto j = start; j < prevLen; j += (numRoutines << 1)) {
                        auto newHash = Config::HashFunc((*buf1)[j], (*buf1)[j + 1]);
                        if (!newHash) {
                            LOG(ERROR) << "Hash failure";
                            break;
                        }
                        (*buf2)[j >> 1] = *newHash;
                    }
                    countdown.signal();
                });
            }
            buf1 = buf2;
            proofGenBufList.push_back(std::make_unique<std::vector<HashString>>(prevLen >> 1));
            buf2 = proofGenBufList.back().get();
            prevLen >>= 1;
            countdown.wait();
            // do not modify buf1 until all workers finished.
            pmt::MerkleTree::fixOdd(*buf1, prevLen);
            this->updateProofsParallel(*buf1, prevLen, step);
        }
        auto newHash = Config::HashFunc((*buf1)[0], (*buf1)[1]);
        if (!newHash) {
            return false;
        }
        Root = *newHash;
        return true;
    }

    void MerkleTree::updateProofsParallel(const std::vector<HashString> &buf, int bufLen, int step) {
        auto batch = 1 << step;

        auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, bufLen);
        bthread::CountdownEvent countdown(numRoutines);
        for (auto i = 0; i < numRoutines; i++) {
            push_task([&, start=i << 1] {
                for (auto j = start; j < bufLen; j += (numRoutines << 1)) {
                    this->updatePairProof(buf, j, batch, step);
                }
                countdown.signal();
            });
        }
        countdown.wait();
    }

    bool MerkleTree::treeBuild() {
        const auto numLeaves = Leaves.size();
        bthread::CountdownEvent future(1);
        push_task([&]() {
            for (auto i = 0; i < (int) Leaves.size(); i++) {
                leafMap[std::string(Leaves[i].begin(), Leaves[i].end())] = i;
            }
            future.signal();
        });
        this->tree = std::vector<std::vector<HashString>>(Depth);
        this->tree[0] = Leaves;
        int prevLen = (int) numLeaves;
        pmt::MerkleTree::fixOdd(tree[0], prevLen);

        for (uint32_t i = 0; i < Depth - 1; i++) {
            this->tree[i + 1] = std::vector<HashString>(prevLen >> 1);
            if (config.RunInParallel) {
                auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, prevLen);
                bthread::CountdownEvent countdown(numRoutines);
                for (auto j = 0; j < numRoutines; j++) {
                    // ----in the original version, numRoutines==config::NumRoutines----
                    push_task([this, start=j << 1, prevLen=prevLen, numRoutines=numRoutines, depth=i, &countdown]{
                        for (auto k = start; k < prevLen; k += (numRoutines << 1)) {
                            auto ret = Config::HashFunc(this->tree[depth][k], this->tree[depth][k + 1]);
                            if (!ret) {
                                LOG(ERROR) << "Hash func failed";
                                break;
                            }
                            this->tree[depth + 1][k >> 1] = *ret;
                        }
                        countdown.signal();
                    });
                    // -----------------------------------------------------------------
                }
                countdown.wait();
            } else {
                for (auto j = 0; j < prevLen; j += 2) {
                    auto ret = Config::HashFunc(tree[i][j], tree[i][j + 1]);
                    if (!ret) {
                        return false;
                    }
                    this->tree[i + 1][j >> 1] = *ret;
                }
            }
            prevLen = (int) this->tree[i + 1].size();
            pmt::MerkleTree::fixOdd(this->tree[i + 1], prevLen);
        }
        auto ret = Config::HashFunc(tree[Depth - 1][0], tree[Depth - 1][1]);
        if (!ret) {
            return false;
        }
        this->Root = *ret;
        future.wait();
        return true;
    }

    std::optional<bool> MerkleTree::Verify(const DataBlock &dataBlock, const Proof &proof, const HashString &root) {
        auto ret2 = Config::HashFunc(dataBlock.Serialize());
        if (!ret2) {
            LOG(ERROR) << "Call hash func error";
            return std::nullopt;
        }
        auto hash(*ret2);
        auto path = proof.Path;
        for (const auto &n: proof.Siblings) {
            if ((path & 1) == 1) {  // hash leaf 1 before leaf 0
                ret2 = Config::HashFunc(hash, *n);
            } else {
                ret2 = Config::HashFunc(*n, hash);
            }
            if (!ret2) {
                return std::nullopt;
            }
            hash = *ret2;
            path >>= 1;
        }
        return hash == root;
    }

    std::optional<Proof> MerkleTree::GenerateProof(const DataBlock &dataBlock) const {
        if (config.Mode != ModeType::ModeTreeBuild && config.Mode != ModeType::ModeProofGenAndTreeBuild) {
            LOG(WARNING) << "merkle Tree is not in built, could not generate proof by this method";
            return std::nullopt;
        }
        auto blockHash = Config::HashFunc(dataBlock.Serialize());
        if (!blockHash) {
            return std::nullopt;
        }
        auto swBlockHash = std::string(blockHash->begin(), blockHash->end());
        if (!leafMap.contains(swBlockHash)) {
            LOG(WARNING) << "data block is not a member of the Merkle Tree";
            return std::nullopt;
        }
        auto idx = leafMap.at(swBlockHash);
        uint32_t path = 0;
        std::vector<const HashString*> siblings(Depth);
        for (uint32_t i = 0; i < Depth; i++) {
            if ((idx & 1) == 1) {
                siblings[i] = &tree[i][idx - 1];
            } else {
                path += (1 << i);
                siblings[i] = &tree[i][idx + 1];
            }
            idx >>= 1;
        }
        return Proof {
                .Siblings = std::move(siblings),
                .Path = path,
        };
    }
}