//
// Created by peng on 11/22/22.
//

#pragma once

#include "common/thread_pool_light.h"
#include "common/crypto.h"
#include "common/phmap.h"
#include <functional>
#include <vector>

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

    // eliminate additional copy, DataBlock need to store the actual string instead
    using ByteString = std::string_view;

    using HashString = util::OpenSSLSHA256::digestType;

    // DataBlock is the interface of input data blocks to generate the Merkle Tree.
    class DataBlock {
    public:
        virtual ~DataBlock() = default;

        [[nodiscard]] virtual HashString Digest() const {
            auto str = Serialize();
            auto digest = util::OpenSSLSHA256::generateDigest(str.data(), str.size());
            CHECK(digest != std::nullopt) << "Can not generate digest.";
            return *digest;
        }

        [[nodiscard]] virtual ByteString Serialize() const = 0;
    };


    // Config is the configuration of Merkle Tree.
    struct Config {
        // HashFuncType is the signature of the hash functions used for Merkle Tree generation.
        // Customizable hash function used for tree generation.
        static HashString HashFunc(const HashString& h1, const HashString& h2);

        // Number of goroutines run in parallel.
        // If RunInParallel is true and NumRoutine is set to 0, use number of CPU as the number of goroutines.
        int NumRoutines = 0;
        // Mode of the Merkle Tree generation.
        ModeType Mode = ModeType::ModeProofGen;
        // If RunInParallel is true, the generation runs in parallel, otherwise runs without parallelization.
        // This increase the performance for the calculation of large number of data blocks, e.g. over 10,000 blocks.
        bool RunInParallel = false;
        bool LeafGenParallel = false;
    };

    // Proof implements the Merkle Tree proof.
    struct Proof {
        std::vector<const HashString*> Siblings{}; // sibling nodes to the Merkle Tree path of the data block
        uint32_t Path{};        // path variable indicating whether the neighbor is on the left or right

        [[nodiscard]] inline bool equal(const Proof& rhs) const {
            return  Path==rhs.Path && rhs.Siblings.size()==Siblings.size() && [&]() ->auto {
                for(auto i=0; i< (int)rhs.Siblings.size(); i++) {
                    if(*this->Siblings[i] != *rhs.Siblings[i])
                        return false;
                }
                return true;
            }();
        }

        [[nodiscard]] std::string toString() const;
    };

    // MerkleTree implements the Merkle Tree structure
    class MerkleTree {
    private:
        Config config;
        // leafMap is the map of the leaf hash to the index in the Tree slice,
        // only available when config mode is ModeTreeBuild or ModeProofGenAndTreeBuild
        util::MyFlatHashMap<std::string, int> leafMap;
        // tree is the Merkle Tree structure, only available when config mode is ModeTreeBuild or ModeProofGenAndTreeBuild
        std::vector<std::vector<HashString>> tree;
        // Root is the Merkle root hash
        HashString Root{};
        // Leaves are Merkle Tree leaves, i.e. the hashes of the data blocks for tree generation
        std::vector<HashString> Leaves;
        // Proofs are proofs to the data blocks generated during the tree building process
        std::vector<Proof> Proofs;
        // Depth is the Merkle Tree depth
        uint32_t Depth{};
        // thread pool
        std::shared_ptr<util::thread_pool_light> wp = nullptr;
        // if wp is created locally, free wp when destruct
        std::unique_ptr<util::thread_pool_light> wpGuard = nullptr;

        // copy-on-write, save time
        std::vector<std::unique_ptr<std::vector<HashString>>> proofGenBufList;
    public:
        [[nodiscard]] inline const auto& getRoot() const { return Root; }

        [[nodiscard]] inline const auto& getProofs() const { return Proofs; }

    protected:
        explicit MerkleTree(const Config &c) : config(c) { }

    public:
        ~MerkleTree() = default;

        MerkleTree(const MerkleTree&) = delete;

        // New generates a new Merkle Tree with specified configuration.
        static std::unique_ptr<MerkleTree> New(const Config &c,
                                               const std::vector<std::unique_ptr<DataBlock>> &blocks,
                                               std::shared_ptr<util::thread_pool_light> wpPtr=nullptr);

    protected:
        template <typename F, typename... A>
        void push_task(F&& task, A&&... args) {
            if (wp != nullptr) {
                wp->push_task(std::forward<F>(task), std::forward<A>(args)...);
            } else {
                task(std::forward<A>(args)...);
            }
        }

        // calTreeDepth calculates the tree depth,
        // the tree depth is then used to declare the capacity of the proof slices.
        static uint32_t calTreeDepth(int blockLen);

        inline void initProofs() {
            const auto numLeaves = Leaves.size();
            Proofs.resize(numLeaves);
            for (auto i = 0; i < (int) numLeaves; i++) {
                Proofs[i].Siblings.reserve(Depth);
            }
        }

        void proofGen();

        // if the length of the buffer calculating the Merkle Tree is odd, then append a node to the buffer
        // if AllowDuplicates is true, append a node by duplicating the previous node
        // otherwise, append a node by random
        static void fixOdd(std::vector<HashString> &buf, int &prevLen);


        inline void updateProofs(const std::vector<HashString> &buf, int bufLen, int step) {
            auto batch = 1 << step;
            for (auto i = 0; i < bufLen; i += 2) {
                this->updatePairProof(buf, i, batch, step);
            }
        }

        void updatePairProof(const std::vector<HashString> &buf, int idx, int batch, int step);

        void leafGen(const std::vector<std::unique_ptr<DataBlock>> &blocks);

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

        void leafGenParallel(const std::vector<std::unique_ptr<DataBlock>> &blocks);

        void proofGenParallel();

        void updateProofsParallel(const std::vector<HashString> &buf, int bufLen, int step);

        void treeBuild();

    public:
        // Verify verifies the data block with the Merkle Tree proof
        [[nodiscard]] inline std::optional<bool> Verify(const DataBlock &dataBlock, const Proof &proof) const {
            return Verify(dataBlock, proof, Root);
        }

        // Verify verifies the data block with the Merkle Tree proof and Merkle root hash
        static std::optional<bool> Verify(const DataBlock &dataBlock, const Proof &proof, const HashString &root);

        // GenerateProof generates the Merkle proof for a data block with the Merkle Tree structure generated beforehand.
        // The method is only available when the configuration mode is ModeTreeBuild or ModeProofGenAndTreeBuild.
        // In ModeProofGen, proofs for all the data blocks are already generated, and the Merkle Tree structure is not cached.
        [[nodiscard]] std::optional<Proof> GenerateProof(const DataBlock &dataBlock) const;
    };
}
