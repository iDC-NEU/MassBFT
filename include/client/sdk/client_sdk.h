//
// Created by user on 23-7-12.
//

#pragma once

#include "proto/block.h"
#include <memory>

namespace util {
    class Properties;
}

namespace client::sdk {
    struct ClientSDKImpl;

    struct BlockHeaderProof {
        // the header of a block
        proto::Block::Header header;
        // the signature for the merkle roots
        proto::Block::Metadata metadata;
    };

    struct ProofLikeStruct {
        std::vector<util::OpenSSLSHA256::digestType> Siblings{};
        uint32_t Path{};
    };

    bool deserializeFromString(const std::string& raw, ProofLikeStruct& ret, int startPos = 0);

    struct TxMerkleProof {
        std::unique_ptr<proto::Envelop> envelop;
        std::unique_ptr<proto::TxReadWriteSet> rwSet;
        std::byte valid;
        // validate if the tx is in the block
        ProofLikeStruct envelopProof;
        // validate if the execResult is in the block
        ProofLikeStruct rwSetProof;
    };

    class SendInterface {
    public:
        virtual ~SendInterface() = default;

        [[nodiscard]] virtual std::unique_ptr<proto::Envelop> invokeChaincode(std::string ccName, std::string funcName, std::string args) const = 0;

    protected:
        SendInterface() = default;
    };

    class ReceiveInterface {
    public:
        virtual ~ReceiveInterface() = default;

        // block id start from 0
        // if return -1, the entire chain is empty
        [[nodiscard]] virtual int getChainHeight(int chainId, int timeoutMs) const = 0;

        [[nodiscard]] virtual std::unique_ptr<BlockHeaderProof> getBlockHeader(int chainId, int blockId, int timeoutMs) const = 0;

        // envelopProof and rwSetProof leaves empty
        [[nodiscard]] virtual std::unique_ptr<TxMerkleProof> getTransaction(const proto::DigestString &txId,
                                                                            int chainIdHint,
                                                                            int blockIdHint,
                                                                            int timeoutMs) const = 0;

        [[nodiscard]] virtual std::unique_ptr<proto::Block> getBlock(int chainId, int blockId, int timeoutMs) const = 0;

        static bool ValidateUserRequestMerkleProof(const proto::HashString &root, const ProofLikeStruct& proof, const std::string& dataBlock);

        static bool ValidateExecResultMerkleProof(const proto::HashString &root, const ProofLikeStruct& proof, const std::string& dataBlock);

    protected:
        ReceiveInterface() = default;
    };

    class ClientSDK : public SendInterface, public ReceiveInterface {
    public:
        static void InitSDKDependencies();

        static std::unique_ptr<ClientSDK> NewSDK(const util::Properties &p);

        ~ClientSDK() override;

        bool connect();

    protected:
        [[nodiscard]] std::unique_ptr<proto::Envelop> invokeChaincode(std::string ccName, std::string funcName, std::string args) const override;

        [[nodiscard]] int getChainHeight(int chainId, int timeoutMs) const override;

        [[nodiscard]] std::unique_ptr<BlockHeaderProof> getBlockHeader(int chainId, int blockId, int timeoutMs) const override;

        [[nodiscard]] std::unique_ptr<TxMerkleProof> getTransaction(const proto::DigestString &txId,
                                                                    int chainIdHint,
                                                                    int blockIdHint,
                                                                    int timeoutMs) const override;

        [[nodiscard]] std::unique_ptr<proto::Block> getBlock(int chainId, int blockId, int timeoutMs) const override;

    protected:
        ClientSDK();

    private:
        std::unique_ptr<ClientSDKImpl> _impl;
    };
}