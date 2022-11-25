//
// Created by peng on 11/22/22.
//

#pragma once

#include "ankerl/unordered_dense.h"
#include "common/thread_pool_light.h"
#include "common/crypto.h"
#include "glog/logging.h"
#include <vector>
#include <functional>
#include <cmath>
#include <bitset>

namespace pmt {

    // ModeType is the type in the Merkle Tree configuration indicating what operations are performed.
    enum class ModeType {
        // ModeProofGen is the proof generation configuration mode.
        ModeProofGen,
        // ModeTreeBuild is the tree building configuration mode.
        ModeTreeBuild,
        // ModeProofGenAndTreeBuild is the proof generation and tree building configuration mode.
        ModeProofGenAndTreeBuild,
    };

    // Default hash result length using SHA256.
    constexpr int defaultHashLen = 32;

    using byteString = std::vector<uint8_t>;

    using hashString = std::array<uint8_t, defaultHashLen>;

    // DataBlock is the interface of input data blocks to generate the Merkle Tree.
    class DataBlock {
    public:
        virtual ~DataBlock() = default;

        [[nodiscard]] virtual std::optional<byteString> Serialize() const = 0;
    };


    // Config is the configuration of Merkle Tree.
    struct Config {
        // HashFuncType is the signature of the hash functions used for Merkle Tree generation.
        // Customizable hash function used for tree generation.
        static std::optional<hashString> HashFunc(const hashString& h1, const hashString& h2) {
            util::OpenSSLSHA256 hash;

            if (!hash.update(h1.data(), h1.size()) ||
                    !hash.update(h2.data(), h2.size()) ) {
                return std::nullopt;
            }
            return hash.final();
        }

        // HashFuncType is the signature of the hash functions used for Merkle Tree generation.
        // Customizable hash function used for tree generation.
        static std::optional<hashString> HashFunc(const byteString& str) {
            util::OpenSSLSHA256 hash;
            if (!hash.update(str.data(), str.size())) {
                return std::nullopt;
            }
            return hash.final();
        }

        // Number of goroutines run in parallel.
        // If RunInParallel is true and NumRoutine is set to 0, use number of CPU as the number of goroutines.
        int NumRoutines = 0;
        // Mode of the Merkle Tree generation.
        ModeType Mode = ModeType::ModeProofGen;
        // If RunInParallel is true, the generation runs in parallel, otherwise runs without parallelization.
        // This increase the performance for the calculation of large number of data blocks, e.g. over 10,000 blocks.
        bool RunInParallel = false;
        // If true, generate a dummy node with random hash value.
        // Otherwise, then the odd node situation is handled by duplicating the previous node.
        bool NoDuplicates = false;
    };

    // Proof implements the Merkle Tree proof.
    struct Proof {
        std::vector<const hashString*> Siblings{}; // sibling nodes to the Merkle Tree path of the data block
        uint32_t Path{};        // path variable indicating whether the neighbor is on the left or right

        bool operator==(const Proof& rhs) const {
            return  Path==rhs.Path && rhs.Siblings.size()==Siblings.size() && [&]->auto {
                for(auto i=0; i< (int)rhs.Siblings.size(); i++) {
                    if(*this->Siblings[i] != *rhs.Siblings[i])
                        return false;
                }
                return true;
            }();
        }

        [[nodiscard]] inline bool equal(const Proof& rhs) const { return *this==rhs; }

        [[nodiscard]] std::string toString() const {
            std::stringstream buf;
            buf << "Path: " << std::bitset<8>(this->Path) <<", Siblings: ";
            for(auto i=0; i<(int)Siblings.size(); i++) {
                buf << "\n\t" << "Siblings depth: "<< Siblings.size()-i << "\t" << util::OpenSSLSHA256::toString(*Siblings[i]);
            }
            return buf.str();
        }
    };

    class MerkleTree;

    // MerkleTree implements the Merkle Tree structure
    class MerkleTree {
    private:
        Config config;
        // leafMap is the map of the leaf hash to the index in the Tree slice,
        // only available when config mode is ModeTreeBuild or ModeProofGenAndTreeBuild
        ankerl::unordered_dense::map<std::string, int> leafMap;
        // tree is the Merkle Tree structure, only available when config mode is ModeTreeBuild or ModeProofGenAndTreeBuild
        std::vector<std::vector<hashString>> tree;
        // Root is the Merkle root hash
        hashString Root{};
        // Leaves are Merkle Tree leaves, i.e. the hashes of the data blocks for tree generation
        std::vector<hashString> Leaves;
        // Proofs are proofs to the data blocks generated during the tree building process
        std::vector<Proof> Proofs;
        // Depth is the Merkle Tree depth
        uint32_t Depth{};
        // thread pool
        util::thread_pool_light* wp = nullptr;
        // if wp is created locally, free wp when destruct
        std::unique_ptr<util::thread_pool_light> wpGuard = nullptr;

        // copy-on-write, save time
        std::vector<std::unique_ptr<std::vector<hashString>>> proofGenBufList;
    public:
        [[nodiscard]] inline const auto& getRoot() const { return Root; }

        [[nodiscard]] inline const auto& getProofs() const { return Proofs; }

    protected:
        explicit MerkleTree(const Config &c) : config(c) { }

    public:
        ~MerkleTree() = default;

        MerkleTree(const MerkleTree&) = delete;

        // New generates a new Merkle Tree with specified configuration.
        static std::unique_ptr<MerkleTree> New(const Config &c, const std::vector<std::unique_ptr<DataBlock>> &blocks, util::thread_pool_light* wpPtr=nullptr) {
            if (blocks.size() <= 1) {
                LOG(ERROR) << "the number of data blocks must be greater than 1";
                return nullptr;
            }
            auto mt = std::unique_ptr<MerkleTree>(new MerkleTree(c));
            // If NumRoutines is unset, then set NumRoutines to the number of CPU.
            if (mt->config.NumRoutines == 0) {
                mt->config.NumRoutines = (int) sysconf(_SC_NPROCESSORS_ONLN);
            }
            // task channel capacity is passed as 0, so use the default value: 2 * numWorkers
            if(wpPtr == nullptr) {
                mt->wpGuard = std::make_unique<util::thread_pool_light>(mt->config.NumRoutines);
                mt->wp = mt->wpGuard.get();
            } else {
                mt->wp = wpPtr;
            }
            mt->Depth = calTreeDepth((int) blocks.size());
            if (mt->config.RunInParallel) {
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

    protected:
        // calTreeDepth calculates the tree depth,
        // the tree depth is then used to declare the capacity of the proof slices.
        static auto calTreeDepth(int blockLen) -> uint32_t {
            auto log2BlockLen = log2(double(blockLen));
            // check if log2BlockLen is an integer
            if (log2BlockLen != int(log2BlockLen)) {
                return uint32_t(log2BlockLen) + 1;
            }
            return uint32_t(log2BlockLen);
        }

        void initProofs() {
            const auto numLeaves = Leaves.size();
            Proofs.resize(numLeaves);
            for (auto i = 0; i < (int) numLeaves; i++) {
                Proofs[i].Siblings.reserve(Depth);
            }
        }

        bool proofGen() {
            const int numLeaves = (int) Leaves.size();
            initProofs();
            proofGenBufList.push_back(std::make_unique<std::vector<hashString>>(Leaves));
            std::vector<hashString>* buf = proofGenBufList.back().get();
            int prevLen = numLeaves;
            this->fixOdd(*buf, prevLen);
            this->updateProofs(*buf, numLeaves, 0);

            for (auto step = 1; step < int(Depth); step++) {
                proofGenBufList.push_back(std::make_unique<std::vector<hashString>>(*buf));
                buf = proofGenBufList.back().get();  // must re-create buf, copy-on-write
                for (auto idx = 0; idx < prevLen; idx += 2) {
                    auto res = Config::HashFunc((*buf)[idx], (*buf)[idx + 1]);
                    if (!res) {
                        return false;
                    }
                    (*buf)[idx >> 1] = *res;
                }
                prevLen >>= 1;
                this->fixOdd(*buf, prevLen);
                this->updateProofs(*buf, prevLen, step);
            }
            auto res = Config::HashFunc((*buf)[0], (*buf)[1]);
            if (!res) {
                return false;
            }
            Root = *res;
            return true;
        }

        // generate a dummy hash to make odd-length buffer even
        static hashString getDummyHash() {
            hashString dummyBytes;
            for (auto &b: dummyBytes) {
                b = random() % 256;
            }
            return dummyBytes;
        }

        // if the length of the buffer calculating the Merkle Tree is odd, then append a node to the buffer
        // if AllowDuplicates is true, append a node by duplicating the previous node
        // otherwise, append a node by random
        void fixOdd(std::vector<hashString> &buf, int &prevLen) const {
            if ((prevLen & 1) == 0) {
                return;
            }
            hashString appendNode;
            if (config.NoDuplicates) {
                appendNode = getDummyHash();
            } else {
                appendNode = buf[prevLen - 1];
            }
            prevLen++;
            if ((int) buf.size() < prevLen) {
                buf.push_back(appendNode);
            } else {
                buf[prevLen - 1] = appendNode;
            }
        }


        void updateProofs(const std::vector<hashString> &buf, int bufLen, int step) {
            auto batch = 1 << step;
            for (auto i = 0; i < bufLen; i += 2) {
                this->updatePairProof(buf, i, batch, step);
            }
        }


        void updatePairProof(const std::vector<hashString> &buf, int idx, int batch, int step) {
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

        bool leafGen(const std::vector<std::unique_ptr<DataBlock>> &blocks) {
            auto lenLeaves = blocks.size();
            this->Leaves.resize(lenLeaves);

            for (auto i = 0; i < (int) lenLeaves; i++) {
                auto ret = blocks[i]->Serialize();
                if (!ret) {
                    return false;
                }
                auto data = std::move(*ret);
                auto hash = Config::HashFunc(data);
                if (!hash) {
                    return false;
                }
                this->Leaves[i] = *hash;
            }
            return true;
        }

        // If there is too little data to calculate, lower the count of worker threads.
        // factor >= 1
        static inline auto calculateNumRoutine(int numRoutines, int workCount, int factor=16) {
            if (numRoutines > workCount/factor) {
                numRoutines = workCount/factor;
            }
            if (numRoutines == 0) {
                numRoutines = 1;
            }
            return numRoutines;
        }

        bool leafGenParallel(const std::vector<std::unique_ptr<DataBlock>> &blocks) {
            int lenLeaves = (int) blocks.size();
            this->Leaves.resize(lenLeaves);

            auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, lenLeaves);
            auto sema = util::NewSema();
            for (auto i = 0; i < numRoutines; i++) {
                wp->push_task([&, i=i]{
                    leafGenHandler(&this->Leaves, &blocks, i, lenLeaves, numRoutines);
                    sema.signal();
                });
            }
            util::wait_for_sema(sema, numRoutines);
            return true;
        }

        // leafGenHandler generates the leaves in parallel.
        // arg fields:
        //
        //	mt: the Merkle Tree instance
        //	byteField2: leaves
        //	dataBlockField: blocks
        //	intField1: start
        //	intField2: lenLeaves
        //	intField3: numRoutines
        static bool leafGenHandler(std::vector<hashString>* leaves,
                                   const std::vector<std::unique_ptr<DataBlock>>* blocks,
                                   int start,
                                   int lenLeaves,
                                   int numRoutines) {
            for (int i = start; i < lenLeaves; i += numRoutines) {
                auto ret = (*blocks)[i]->Serialize();
                if (!ret) {
                    return false;
                }
                auto data = std::move(*ret);
                auto hash = Config::HashFunc(data);
                if (!hash) {
                    return false;
                }
                (*leaves)[i] = *hash;
            }
            return true;
        }

        // proofGenHandler generates the proofs in parallel.
        // arg fields:
        //
        //	mt: the Merkle Tree instance
        //	byteField1: buf1
        //	byteField2: buf2
        //	intField1: start
        //	intField2: prevLen
        //	intField3: numRoutines
        static bool proofGenHandler(const std::vector<hashString>* buf1,
                                    std::vector<hashString>* buf2,
                                    int start,
                                    int prevLen,
                                    int numRoutines) {
            for (auto i = start; i < prevLen; i += (numRoutines << 1)) {
                auto newHash = Config::HashFunc((*buf1)[i], (*buf1)[i + 1]);
                if (!newHash) {
                    return false;
                }
                (*buf2)[i >> 1] = *newHash;
            }
            return true;
        }

        bool proofGenParallel() {
            this->initProofs();
            const int numLeaves = (int) Leaves.size();
            proofGenBufList.push_back(std::make_unique<std::vector<hashString>>(Leaves));
            std::vector<hashString>* buf1 = proofGenBufList.back().get();  // have to perform deep copy
            int prevLen = numLeaves;
            this->fixOdd(*buf1, prevLen);
            this->updateProofsParallel(*buf1, numLeaves, 0);

            proofGenBufList.push_back(std::make_unique<std::vector<hashString>>(prevLen >> 1));
            std::vector<hashString>* buf2 = proofGenBufList.back().get();
            auto sema = util::NewSema();
            for (auto step = 1; step < int(Depth); step++) {
                auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, prevLen);
                for (auto i = 0; i < numRoutines; i++) {
                    wp->push_task([&, i=i] {
                        proofGenHandler(buf1, buf2, i << 1, prevLen, numRoutines);
                        sema.signal();
                    });

                }
                buf1 = buf2;
                proofGenBufList.push_back(std::make_unique<std::vector<hashString>>(prevLen >> 1));
                buf2 = proofGenBufList.back().get();
                prevLen >>= 1;
                util::wait_for_sema(sema, numRoutines);
                // do not modify buf1 until all workers finished.
                this->fixOdd(*buf1, prevLen);
                this->updateProofsParallel(*buf1, prevLen, step);
            }
            auto newHash = Config::HashFunc((*buf1)[0], (*buf1)[1]);
            if (!newHash) {
                return false;
            }
            Root = *newHash;
            return true;
        }

        // updateProofHandler updates the proofs in parallel.
        // arg fields:
        //
        //	mt: the Merkle Tree instance
        //	byteField1: buf
        //	intField1: start
        //	intField2: batch
        //	intField3: step
        //	intField4: bufLen
        //	intField5: numRoutines
        static bool updateProofHandler(MerkleTree *mt,
                                       const std::vector<hashString>* buf,
                                       int start,
                                       int batch,
                                       int step,
                                       int bufLen,
                                       int numRoutines) {
            for (auto i = start; i < bufLen; i += (numRoutines << 1)) {
                mt->updatePairProof(*buf, i, batch, step);
            }
            // return the nil error to be compatible with the handler type
            return true;
        }

        void updateProofsParallel(const std::vector<hashString> &buf, int bufLen, int step) {
            auto batch = 1 << step;

            auto sema = util::NewSema();
            auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, bufLen);
            for (auto i = 0; i < numRoutines; i++) {
                wp->push_task([&, i=i] {
                    updateProofHandler(this, &buf, i << 1, batch, step, bufLen, numRoutines);
                    sema.signal();
                });
            }
            util::wait_for_sema(sema, numRoutines);
        }

        bool treeBuild() {
            const auto numLeaves = Leaves.size();
            auto future = wp->submit([this] {
                for (auto i = 0; i < (int) Leaves.size(); i++) {
                    leafMap[std::string(Leaves[i].begin(), Leaves[i].end())] = i;
                }
                return true;
            });
            this->tree = std::vector<std::vector<hashString>>(Depth);
            this->tree[0] = Leaves;
            int prevLen = (int) numLeaves;
            this->fixOdd(tree[0], prevLen);

            auto sema = util::NewSema();
            for (uint32_t i = 0; i < Depth - 1; i++) {
                this->tree[i + 1] = std::vector<hashString>(prevLen >> 1);
                if (config.RunInParallel) {
                    auto numRoutines = MerkleTree::calculateNumRoutine(config.NumRoutines, prevLen);
                    for (auto j = 0; j < numRoutines; j++) {
                        // ----in the original version, numRoutines==config::NumRoutines----
                        wp->push_task([this, j=j, prevLen=prevLen, numRoutines=numRoutines, i=i, &sema]{
                            treeBuildHandler(this, j << 1, prevLen, numRoutines, i);
                            sema.signal();
                        });
                        // -----------------------------------------------------------------
                    }
                    util::wait_for_sema(sema, numRoutines);
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
                this->fixOdd(this->tree[i + 1], prevLen);
            }
            auto ret = Config::HashFunc(tree[Depth - 1][0], tree[Depth - 1][1]);
            if (!ret) {
                return false;
            }
            this->Root = *ret;
            return future.get();
        }


        // treeBuildHandler builds the tree in parallel.
        // arg fields:
        //
        //	mt: the Merkle Tree instance
        //	intField1: start
        //	intField2: prevLen
        //	intField3: numRoutines
        //	uint32Field: depth
        static bool treeBuildHandler(MerkleTree *mt,
                                     int start,
                                     int prevLen,
                                     int numRoutines,
                                     uint32_t depth) {
            for (auto i = start; i < prevLen; i += (numRoutines << 1)) {
                auto ret = Config::HashFunc(mt->tree[depth][i], mt->tree[depth][i + 1]);
                if (!ret) {
                    return false;
                }
                mt->tree[depth + 1][i >> 1] = *ret;
            }
            return true;
        }

    public:
        // Verify verifies the data block with the Merkle Tree proof
        [[nodiscard]] std::optional<bool> Verify(const DataBlock &dataBlock, const Proof &proof) const {
            return Verify(dataBlock, proof, Root);
        }

        // Verify verifies the data block with the Merkle Tree proof and Merkle root hash
        static std::optional<bool> Verify(const DataBlock &dataBlock, const Proof &proof, const hashString &root) {
            auto ret = dataBlock.Serialize();
            if (!ret) {
                LOG(ERROR) << "Serialize data block error";
                return std::nullopt;
            }
            auto data = std::move(*ret);

            auto ret2 = Config::HashFunc(data);
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

        // GenerateProof generates the Merkle proof for a data block with the Merkle Tree structure generated beforehand.
        // The method is only available when the configuration mode is ModeTreeBuild or ModeProofGenAndTreeBuild.
        // In ModeProofGen, proofs for all the data blocks are already generated, and the Merkle Tree structure is not cached.
        [[nodiscard]] std::optional<Proof> GenerateProof(const DataBlock &dataBlock) const {
            if (config.Mode != ModeType::ModeTreeBuild && config.Mode != ModeType::ModeProofGenAndTreeBuild) {
                LOG(WARNING) << "merkle Tree is not in built, could not generate proof by this method";
                return std::nullopt;
            }
            auto ret = dataBlock.Serialize();
            if (!ret) {
                return std::nullopt;
            }
            auto blockByte = std::move(*ret);
            auto ret2 = Config::HashFunc(blockByte);
            if (!ret2) {
                return std::nullopt;
            }
            auto blockHash = *ret2;
            auto swBlockHash = std::string(blockHash.begin(), blockHash.end());
            if (!leafMap.contains(swBlockHash)) {
                LOG(WARNING) << "data block is not a member of the Merkle Tree";
                return std::nullopt;
            }
            auto idx = leafMap.at(swBlockHash);
            uint32_t path = 0;
            std::vector<const hashString*> siblings(Depth);
            for (uint32_t i = 0; i < Depth; i++) {
                if ((idx & 1) == 1) {
                    siblings[i] = &tree[i][idx - 1];
                } else {
                    path += (1 << i);
                    siblings[i] = &tree[i][idx + 1];
                }
                idx >>= 1;
            }
            return Proof{
                    .Siblings = std::move(siblings),
                    .Path = path,
            };
        }
    };
}
