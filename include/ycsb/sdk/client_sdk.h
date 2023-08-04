//
// Created by user on 23-7-12.
//

#pragma once

#include "proto/block.h"
#include <memory>

namespace util {
    class Properties;
}

namespace ycsb::sdk {
    struct ClientSDKImpl;

    struct BlockHeaderProof {
        // the header of a block
        proto::Block::Header header;
        // the signature for the merkle roots
        proto::Block::Metadata metadata;
    };

    struct TxMerkleProof {
        std::unique_ptr<proto::Envelop> envelop;
        std::unique_ptr<proto::TxReadWriteSet> rwSet;
        // validate if the tx is in the block
        std::string envelopProof;
        // validate if the execResult is in the block
        std::string rwSetProof;
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

        // return -1 on error
        [[nodiscard]] virtual int getChainHeight(int chainId, int timeoutMs) const = 0;

        [[nodiscard]] virtual std::unique_ptr<BlockHeaderProof> getBlockHeader(int chainId, int blockId, int timeoutMs) const = 0;

        [[nodiscard]] virtual std::unique_ptr<TxMerkleProof> getTxWithProof(const proto::DigestString& txId, int timeoutMs) const = 0;

        // envelopProof and rwSetProof leaves empty
        [[nodiscard]] virtual std::unique_ptr<TxMerkleProof> getTransaction(const proto::DigestString& txId, int timeoutMs) const = 0;

        [[nodiscard]] virtual std::unique_ptr<proto::Block> getBlock(int chainId, int blockId, int timeoutMs) const = 0;

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

        [[nodiscard]] std::unique_ptr<TxMerkleProof> getTxWithProof(const proto::DigestString& txId, int timeoutMs) const override;

        [[nodiscard]] std::unique_ptr<TxMerkleProof> getTransaction(const proto::DigestString& txId, int timeoutMs) const override;

        [[nodiscard]] std::unique_ptr<proto::Block> getBlock(int chainId, int blockId, int timeoutMs) const override;

    protected:
        ClientSDK();

    private:
        std::unique_ptr<ClientSDKImpl> _impl;
    };
}