//
// Created by peng on 11/22/22.
//


#include "gtest/gtest.h"
#include "common/parallel_merkle_tree.h"

class PMTreeTest : public ::testing::Test {
protected:
    void SetUp() override {
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


};

TEST_F(PMTreeTest, TestMerkleTreeNew_proofGen) {
    struct args_type {
        vector_ptr<DataBlockPtr> blocks;
        pmt::Config config;
    };
    struct test_case {
        std::string name;
        args_type args;
        bool wantErr;
    };
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

TEST_F(PMTreeTest, MultiGetStore) {


}
