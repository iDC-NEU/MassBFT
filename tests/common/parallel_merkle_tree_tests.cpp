//
// Created by peng on 11/22/22.
//


#include "gtest/gtest.h"
#include "common/parallel_merkle_tree.h"
#include "common/timer.h"

class PMTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
        util::OpenSSLSHA1::initCrypto();
        util::OpenSSLSHA256::initCrypto();
    };

    void TearDown() override {
    };

    using byteString = pmt::byteString;

    const int benchSize = 10000;

    class mockDataBlock: public pmt::DataBlock {
    public:
        byteString data;

        [[nodiscard]] std::optional<byteString> Serialize() const override {
            return data;
        }
    };

    using DataBlockPtr = std::unique_ptr<pmt::DataBlock>;

    template<class T>
    using vector_ptr = std::unique_ptr<std::vector<T>>;

    static vector_ptr<DataBlockPtr> genTestDataBlocks(int num) {
        auto blocks = std::make_unique<std::vector<DataBlockPtr>>(num);
        for (auto i = 0; i < num; i++) {
            auto block = std::make_unique<mockDataBlock>();
            fillDummy(block->data, 100);
            (*blocks)[i] = std::move(block);
        }
        return blocks;
    }

    static void fillDummy(byteString& dummyBytes, int len) {
        dummyBytes.resize(len);
        for (auto &b: dummyBytes) {
            b = random() % 256;
        }
    }

    struct args_type {
        vector_ptr<DataBlockPtr> blocks;
        pmt::Config config;
    };
    struct test_case {
        std::string name;
        args_type args;
        bool wantErr;
    };

    static auto verifySetup(int size) {
        auto blocks = genTestDataBlocks(size);
        auto mt = pmt::MerkleTree::New({}, *blocks);
        CHECK(mt != nullptr) << "init mt failed";
        return std::make_tuple(std::move(mt), std::move(blocks));
    }

    static auto verifySetupParallel(int size) {
        auto blocks = genTestDataBlocks(size);
        pmt::Config config;
        config.RunInParallel = true;
        config.NumRoutines = 4;
        auto mt = pmt::MerkleTree::New({}, *blocks);
        CHECK(mt != nullptr) << "init mt failed";
        return std::make_tuple(std::move(mt), std::move(blocks));
    }


};

TEST_F(PMTreeTest, TestMerkleTreeNew_proofGen) {
    std::vector<test_case> tests;

    tests.push_back({
                            .name= "test_0",
                            .args= {
                                    .blocks = genTestDataBlocks(0),
                                    .config{},
                            },
                            .wantErr= true,
                    });
    tests.push_back({
                            .name= "test_1",
                            .args= {
                                    .blocks = genTestDataBlocks(1),
                                    .config{},
                            },
                            .wantErr= true,
                    });
    tests.push_back({
                            .name= "test_2",
                            .args= {
                                    .blocks = genTestDataBlocks(2),
                                    .config{},
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_8",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config{},
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_5",
                            .args= {
                                    .blocks = genTestDataBlocks(5),
                                    .config{},
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_1000",
                            .args= {
                                    .blocks = genTestDataBlocks(1000),
                                    .config{},
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_100_parallel",
                            .args= {
                                    .blocks= genTestDataBlocks(100),
                                    .config= {
                                            .NumRoutines=   4,
                                            .Mode = pmt::ModeType::ModeProofGen,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_10_32_parallel",
                            .args= {
                                    .blocks= genTestDataBlocks(10),
                                    .config= {
                                            .NumRoutines=   32,
                                            .Mode = pmt::ModeType::ModeProofGen,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_100_parallel_no_specify_num_of_routines",
                            .args= {
                                    .blocks= genTestDataBlocks(100),
                                    .config= {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGen,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_100_parallel_random",
                            .args= {
                                    .blocks= genTestDataBlocks(100),
                                    .config= {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGen,
                                            .RunInParallel = true,
                                            .NoDuplicates = true,
                                    },
                            },
                            .wantErr= false,
                    });
    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        if (auto mt = pmt::MerkleTree::New(tt.args.config, *tt.args.blocks); (mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }
    }

}

TEST_F(PMTreeTest, TestMerkleTreeNew_buildTree) {
    std::vector<test_case> tests;

    tests.push_back({
                            .name= "test_build_tree_2",
                            .args= {
                                    .blocks = genTestDataBlocks(2),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_5",
                            .args= {
                                    .blocks = genTestDataBlocks(5),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_8",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_build_tree_1000",
                            .args= {
                                    .blocks = genTestDataBlocks(1000),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        auto mt = pmt::MerkleTree::New(tt.args.config, *tt.args.blocks);

        if ((mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }

        auto m1 = pmt::MerkleTree::New({}, *tt.args.blocks);
        if (m1 == nullptr) {
            ASSERT_TRUE(false) << "test setup error";
        }

        if ((mt->getRoot() != m1->getRoot()) && !tt.wantErr) {
            LOG(ERROR) << "mt Root: " << util::OpenSSLSHA256::toString(mt->getRoot());
            LOG(ERROR) << "m1 Root: " << util::OpenSSLSHA256::toString(m1->getRoot());
            ASSERT_TRUE(false) << "tree generated is wrong";
        }
    }
}

TEST_F(PMTreeTest, TestMerkleTreeNew_treeBuildParallel) {
    std::vector<test_case> tests;

    tests.push_back({
                            .name= "test_build_tree_parallel_2",
                            .args= {
                                    .blocks = genTestDataBlocks(2),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_parallel_4",
                            .args= {
                                    .blocks = genTestDataBlocks(4),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_parallel_5",
                            .args= {
                                    .blocks = genTestDataBlocks(5),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_parallel_8",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_parallel_8_32",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config = {
                                            .NumRoutines = 32,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_parallel_1000_32",
                            .args= {
                                    .blocks = genTestDataBlocks(1000),
                                    .config = {
                                            .NumRoutines = 32,
                                            .Mode = pmt::ModeType::ModeTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });
    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        auto mt = pmt::MerkleTree::New(tt.args.config, *tt.args.blocks);

        if ((mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }

        if (tt.wantErr) {
            continue;
        }

        auto m1 = pmt::MerkleTree::New({}, *tt.args.blocks);
        if (m1 == nullptr) {
            ASSERT_TRUE(false) << "test setup error";
        }

        if ((mt->getRoot() != m1->getRoot()) && !tt.wantErr) {
            LOG(ERROR) << "mt Root: " << util::OpenSSLSHA256::toString(mt->getRoot());
            LOG(ERROR) << "m1 Root: " << util::OpenSSLSHA256::toString(m1->getRoot());
            ASSERT_TRUE(false) << "tree generated is wrong";
        }
    }
}

TEST_F(PMTreeTest, TestMerkleTreeNew_proofGenAndTreeBuild) {
    std::vector<test_case> tests;

    tests.push_back({
                            .name= "test_build_tree_proof_2",
                            .args= {
                                    .blocks = genTestDataBlocks(2),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_4",
                            .args= {
                                    .blocks = genTestDataBlocks(4),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_5",
                            .args= {
                                    .blocks = genTestDataBlocks(5),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_8",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_9",
                            .args= {
                                    .blocks = genTestDataBlocks(9),
                                    .config = {
                                            .NumRoutines = 0,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = false,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        auto mt = pmt::MerkleTree::New(tt.args.config, *tt.args.blocks);

        if ((mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }

        if (tt.wantErr) {
            continue;
        }

        auto m1 = pmt::MerkleTree::New({}, *tt.args.blocks);
        if (m1 == nullptr) {
            ASSERT_TRUE(false) << "test setup error";
        }
        for (auto i = 0; i < (int)tt.args.blocks->size(); i++) {
            const auto& mtProofs = mt->getProofs();
            const auto& m1Proofs = m1->getProofs();
            if (!m1Proofs[i].equal(mtProofs[i])) {
                ASSERT_TRUE(false) << "proofs generated are wrong for block " << i;
            }
        }
    }
}


TEST_F(PMTreeTest, TestMerkleTreeNew_proofGenAndTreeBuildParallel) {
    std::vector<test_case> tests;

    tests.push_back({
                            .name= "test_build_tree_proof_parallel_2",
                            .args= {
                                    .blocks = genTestDataBlocks(2),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_parallel_4",
                            .args= {
                                    .blocks = genTestDataBlocks(4),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_parallel_5",
                            .args= {
                                    .blocks = genTestDataBlocks(5),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_parallel_8",
                            .args= {
                                    .blocks = genTestDataBlocks(8),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    tests.push_back({
                            .name= "test_build_tree_proof_parallel_9",
                            .args= {
                                    .blocks = genTestDataBlocks(9),
                                    .config = {
                                            .NumRoutines = 4,
                                            .Mode = pmt::ModeType::ModeProofGenAndTreeBuild,
                                            .RunInParallel = true,
                                            .NoDuplicates = false,
                                    },
                            },
                            .wantErr= false,
                    });

    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        auto mt = pmt::MerkleTree::New(tt.args.config, *tt.args.blocks);

        if ((mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }

        if (tt.wantErr) {
            continue;
        }

        auto m1 = pmt::MerkleTree::New({}, *tt.args.blocks);
        if (m1 == nullptr) {
            ASSERT_TRUE(false) << "test setup error";
        }
        for (auto i = 0; i < (int)tt.args.blocks->size(); i++) {
            const auto& mtProofs = mt->getProofs();
            const auto& m1Proofs = m1->getProofs();
            if (!m1Proofs[i].equal(mtProofs[i])) {
                ASSERT_TRUE(false) << "proofs generated are wrong for block " << i;
            }
        }
    }
}

TEST_F(PMTreeTest, TestMerkleTree_Verify) {
    struct test_case_verify {
        std::string name;
        bool parallel;
        int blockSize;
        bool want;
        bool wantErr;
    };
    std::vector<test_case_verify> tests;
    tests.push_back({
                            .name= "test_pseudo_random_2",
                            .parallel = false,
                            .blockSize = 2,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_3",
                            .parallel = false,
                            .blockSize = 3,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_4",
                            .parallel = false,
                            .blockSize = 4,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_5",
                            .parallel = false,
                            .blockSize = 5,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_6",
                            .parallel = false,
                            .blockSize = 6,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_8",
                            .parallel = false,
                            .blockSize = 8,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_9",
                            .parallel = false,
                            .blockSize = 9,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_1001",
                            .parallel = false,
                            .blockSize = 1001,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_64_parallel",
                            .parallel = true,
                            .blockSize = 64,
                            .want = true,
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_pseudo_random_1001_parallel",
                            .parallel = true,
                            .blockSize = 1001,
                            .want = true,
                            .wantErr= false,
                    });

    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;
        auto setupFunc = verifySetup;
        if (tt.parallel) {
            setupFunc = verifySetupParallel;
        }
        auto [mt, blocks] = setupFunc(tt.blockSize);
        if ((mt == nullptr) != tt.wantErr) {
            ASSERT_TRUE(false) << "Build() error, want: " << tt.wantErr;
        }

        if (tt.wantErr) {
            continue;
        }

        for (auto i = 0; i < tt.blockSize; i++) {
            auto got = mt->Verify(*(*blocks)[i], mt->getProofs()[i]);
            if ((got == std::nullopt) != tt.wantErr) {
                ASSERT_TRUE(false) << "Verify() error, want: " << tt.wantErr;
            }
            if (got.value() != tt.want) {
                ASSERT_TRUE(false) << "Verify(), want: " << tt.want;
            }
        }
    }
}

TEST_F(PMTreeTest, TestMerkleTree_GenerateProof) {
    struct test_case_gen_proof {
        std::string name;
        pmt::Config config;
        vector_ptr<DataBlockPtr> blocks;
        vector_ptr<DataBlockPtr> proofBlocks;
        bool wantErr;
    };
    auto s = std::string_view("test_wrong_blocks");
    auto mockBlock = std::make_unique<mockDataBlock>();
    std::copy(s.begin(), s.end(), std::back_inserter(mockBlock->data)); // append value

    std::vector<test_case_gen_proof> tests;
    tests.push_back({
                            .name= "test_2",
                            .config = {
                                    .NumRoutines = 0,
                                    .Mode = pmt::ModeType::ModeTreeBuild,
                                    .RunInParallel = false,
                                    .NoDuplicates = false,
                            },
                            .blocks = genTestDataBlocks(2),
                            .proofBlocks = {},
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_4",
                            .config = {
                                    .NumRoutines = 0,
                                    .Mode = pmt::ModeType::ModeTreeBuild,
                                    .RunInParallel = false,
                                    .NoDuplicates = false,
                            },
                            .blocks = genTestDataBlocks(4),
                            .proofBlocks = {},
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_5",
                            .config = {
                                    .NumRoutines = 0,
                                    .Mode = pmt::ModeType::ModeTreeBuild,
                                    .RunInParallel = false,
                                    .NoDuplicates = false,
                            },
                            .blocks = genTestDataBlocks(5),
                            .proofBlocks = {},
                            .wantErr= false,
                    });
    tests.push_back({
                            .name= "test_wrong_mode",
                            .config = {
                                    .NumRoutines = 0,
                                    .Mode = pmt::ModeType::ModeProofGen,
                                    .RunInParallel = false,
                                    .NoDuplicates = false,
                            },
                            .blocks = genTestDataBlocks(5),
                            .proofBlocks = {},
                            .wantErr= true,
                    });
    tests.push_back({
                            .name= "test_wrong_blocks",
                            .config = {
                                    .NumRoutines = 0,
                                    .Mode = pmt::ModeType::ModeTreeBuild,
                                    .RunInParallel = false,
                                    .NoDuplicates = false,
                            },
                            .blocks = genTestDataBlocks(5),
                            .proofBlocks = {},
                            .wantErr= true,
                    });
    tests.back().proofBlocks = std::make_unique<std::vector<DataBlockPtr>>();
    tests.back().proofBlocks->push_back(std::move(mockBlock));

    for (const auto& tt : tests) {
        LOG(INFO) << "testing " << tt.name;

        auto m1 = pmt::MerkleTree::New({}, *tt.blocks);
        if (m1 == nullptr) {
            ASSERT_TRUE(false) << "m1 New() error";
        }

        auto m2 = pmt::MerkleTree::New(tt.config, *tt.blocks);
        if (m2 == nullptr) {
            ASSERT_TRUE(false) << "m2 New() error";
        }
        auto* proofBlockRef = tt.proofBlocks.get();
        if (tt.proofBlocks == nullptr) {
            proofBlockRef = tt.blocks.get();
        }

        for (int i=0; i<(int)proofBlockRef->size(); i++) {
            auto ret = m2->GenerateProof(*(*proofBlockRef)[i]);
            if ((ret == std::nullopt) != tt.wantErr) {
                ASSERT_TRUE(false) << "GenerateProof() error, wantErr: " << tt.wantErr;
            }
            if (tt.wantErr) {
                continue;
            }

            if ((ret.value()!=m1->getProofs()[i]) && !tt.wantErr) {
                ASSERT_TRUE(false) << "GenerateProof() "<< i <<
                                   ", got: " << ret.value().toString() <<
                                   ", want: " << m1->getProofs()[i].toString();
            }
        }
    }
}

TEST_F(PMTreeTest, TestVerify) {
    auto blocks = genTestDataBlocks(5);
    auto mt = pmt::MerkleTree::New({}, *blocks);
    if(!mt) {
        ASSERT_TRUE(false) << "init error";
    }
    struct shared_test_case {
        std::string name;
        pmt::DataBlock* dataBlock;
        pmt::Proof proof;
        pmt::hashString root;
        bool want;
    };
    std::vector<shared_test_case> tests;

    tests.push_back({
                            .name= "test_ok",
                            .dataBlock = (*blocks)[0].get(),
                            .proof = mt->getProofs()[0],
                            .root = mt->getRoot(),
                            .want= true,
                    });

    tests.push_back({
                            .name= "test_wrong_root",
                            .dataBlock = (*blocks)[0].get(),
                            .proof = mt->getProofs()[0],
                            .root{},
                            .want = false,
                    });

    auto badRoot = std::string_view("test_wrong_root");
    std::copy_n(badRoot.begin(), 32, tests.back().root.begin());

    for (const auto& tt : tests) {
        auto result = pmt::MerkleTree::Verify(*tt.dataBlock, tt.proof, tt.root);

        if (result == std::nullopt) {
            ASSERT_TRUE(false) << "Verify()) error";
        }
        if (*result != tt.want) {
            ASSERT_TRUE(false) << "Verify()) error, want: " << tt.want;
        }
    }
}

// 35.6ms in go, 13.1ms in c++
TEST_F(PMTreeTest, BenchmarkMerkleTreeNew) {
    auto testCases = genTestDataBlocks(benchSize);
    pmt::Config config;
    auto wp = std::make_unique<dp::thread_pool<>>((int) sysconf(_SC_NPROCESSORS_ONLN) / 2);
    util::Timer timer;
    for(int i=0; i<100; i++) {
        pmt::MerkleTree::New(config, *testCases, wp.get());
    }
    LOG(INFO) << "BenchmarkMerkleTreeNew 100 run costs: " << timer.end();
}

// 17.1ms in go, 18.5ms in c++
TEST_F(PMTreeTest, BenchmarkMerkleTreeNewParallel) {
    auto testCases = genTestDataBlocks(benchSize);
    pmt::Config config;
    config.RunInParallel = true;
    auto wp = std::make_unique<dp::thread_pool<>>((int) sysconf(_SC_NPROCESSORS_ONLN) / 2);
    util::Timer timer;
    for(int i=0; i<100; i++) {
        pmt::MerkleTree::New(config, *testCases, wp.get());
    }
    LOG(INFO) << "BenchmarkMerkleTreeNewParallel 100 run costs: " << timer.end();
}

// 31.1ms in go, 10.2ms in c++
TEST_F(PMTreeTest, BenchmarkMerkleTreeBuild) {
    auto testCases = genTestDataBlocks(benchSize);
    pmt::Config config;
    config.Mode = pmt::ModeType::ModeTreeBuild;
    auto wp = std::make_unique<dp::thread_pool<>>((int) sysconf(_SC_NPROCESSORS_ONLN) / 2);
    util::Timer timer;
    for(int i=0; i<100; i++) {
        pmt::MerkleTree::New(config, *testCases, wp.get());
    }
    LOG(INFO) << "BenchmarkMerkleTreeBuild 100 run costs: " << timer.end();
}

// 17.5ms in go, 11.2ms in c++
TEST_F(PMTreeTest, BenchmarkMerkleTreeBuildParallel) {
    auto testCases = genTestDataBlocks(benchSize);
    pmt::Config config;
    config.Mode = pmt::ModeType::ModeTreeBuild;
    config.RunInParallel = true;
    auto wp = std::make_unique<dp::thread_pool<>>((int) sysconf(_SC_NPROCESSORS_ONLN) / 2);
    util::Timer timer;
    for(int i=0; i<100; i++) {
        pmt::MerkleTree::New(config, *testCases, wp.get());
    }
    LOG(INFO) << "BenchmarkMerkleTreeBuildParallel 100 run costs: " << timer.end();
}

// 17.5ms in go, 11.2ms in c++
TEST_F(PMTreeTest, SimpleTest) {
    auto genData = []()-> vector_ptr<DataBlockPtr> {
        auto blocks = std::make_unique<std::vector<DataBlockPtr>>(4);
        for (auto i = 0; i < 4; i++) {
            auto block = std::make_unique<mockDataBlock>();
            const auto data = "data" + std::to_string(i);
            std::copy(data.begin(), data.end(), std::back_inserter(block->data));
            (*blocks)[i] = std::move(block);
        }
        return blocks;
    };
    auto testCases = genData();
    pmt::Config config;
    config.Mode = pmt::ModeType::ModeProofGenAndTreeBuild;
    config.RunInParallel = true;
    auto wp = std::make_unique<dp::thread_pool<>>((int) sysconf(_SC_NPROCESSORS_ONLN) / 2);
    auto mt = pmt::MerkleTree::New(config, *testCases, wp.get());
    for (auto i = 0; i < (int)testCases->size(); i++) {
        auto got = mt->Verify(*(*testCases)[i], mt->getProofs()[i]);
        if (got == std::nullopt) {
            ASSERT_TRUE(false) << "Verify() error";
        }
        if (!got.value()) {
            ASSERT_TRUE(false) << "Verify() result err";
        }
    }
    const auto& proofs = mt->getProofs();
    util::OpenSSLSHA256 sha256;
    auto leaf00 = (*testCases)[0]->Serialize().value();
    sha256.update({reinterpret_cast<const char *>(leaf00.data()), leaf00.size()});
    auto res00 = sha256.final().value();
    auto leaf01 = (*testCases)[1]->Serialize().value();
    sha256.update({reinterpret_cast<const char *>(leaf01.data()), leaf01.size()});
    auto res01 = sha256.final().value();

    auto leaf10 = (*testCases)[2]->Serialize().value();
    sha256.update({reinterpret_cast<const char *>(leaf10.data()), leaf10.size()});
    auto res10 = sha256.final().value();
    auto leaf11 = (*testCases)[3]->Serialize().value();
    sha256.update({reinterpret_cast<const char *>(leaf11.data()), leaf11.size()});
    auto res11 = sha256.final().value();

    sha256.update({reinterpret_cast<const char *>(res00.data()), res00.size()});
    sha256.update({reinterpret_cast<const char *>(res01.data()), res01.size()});
    auto res0 = sha256.final().value();

    sha256.update({reinterpret_cast<const char *>(res10.data()), res10.size()});
    sha256.update({reinterpret_cast<const char *>(res11.data()), res11.size()});
    auto res1 = sha256.final().value();

    sha256.update({reinterpret_cast<const char *>(res0.data()), res0.size()});
    sha256.update({reinterpret_cast<const char *>(res1.data()), res1.size()});
    auto final = sha256.final().value();
    if (final != mt->getRoot()) {
        LOG(INFO) << "----Expect Proofs----";
        LOG(INFO) << proofs[0].toString();
        LOG(INFO) << proofs[1].toString();
        LOG(INFO) << proofs[2].toString();
        LOG(INFO) << proofs[3].toString();
        LOG(INFO) << "----Expect Root----";
        LOG(INFO) << util::OpenSSLSHA256::toString(mt->getRoot());
        LOG(INFO) << "----Level2----";
        LOG(INFO) << util::OpenSSLSHA256::toString(res00);
        LOG(INFO) << util::OpenSSLSHA256::toString(res01);
        LOG(INFO) << util::OpenSSLSHA256::toString(res10);
        LOG(INFO) << util::OpenSSLSHA256::toString(res11);
        LOG(INFO) << "----Level1----";
        LOG(INFO) << util::OpenSSLSHA256::toString(res0);
        LOG(INFO) << util::OpenSSLSHA256::toString(res1);
        LOG(INFO) << "----Level0----";
        LOG(INFO) << util::OpenSSLSHA256::toString(final);
        ASSERT_TRUE(false) << "Merkle tree generation error";
    }
}
