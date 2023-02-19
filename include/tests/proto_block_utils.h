//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"

namespace tests {
    class ProtoBlockUtils {
    public:
        static std::unique_ptr<proto::Block> CreateDemoBlock() {
            std::unique_ptr<proto::Block> realBlock(new proto::Block);
            proto::Block& b = *realBlock;
            b.header.dataHash = {"dataHash"};
            b.header.previousHash = {"previousHash"};
            b.header.number = 10;
            std::vector<std::unique_ptr<proto::TxReadWriteSet>> rwSets;
            for(int i=0; i<5; i++) {
                std::unique_ptr<proto::TxReadWriteSet> rwSet(new proto::TxReadWriteSet("test rw set"));
                std::unique_ptr<proto::KV> read(new proto::KV("key1", "value1"));
                rwSet->getReads().push_back(std::move(read));
                std::unique_ptr<proto::KV> write(new proto::KV("key2", "value2"));
                rwSet->getWrites().push_back(std::move(write));
                rwSet->setRetCode(1);
                rwSets.push_back(std::move(rwSet));
            }
            b.executeResult.txReadWriteSet = std::move(rwSets);
            b.executeResult.transactionFilter.resize(2);
            b.executeResult.transactionFilter[1] = (std::byte)10;
            proto::SignatureString sig1 = {"ski", std::make_shared<std::string>("public key1"), {"sig1"}};
            proto::SignatureString sig2 = {"ski", std::make_shared<std::string>("public key2"), {"sig2"}};
            b.metadata.consensusSignatures.emplace_back(sig1);
            b.metadata.consensusSignatures.emplace_back(sig2);

            proto::SignatureString sig3 = {"ski", std::make_shared<std::string>("public key3"), {"sig3"}};
            proto::SignatureString sig4 = {"ski", std::make_shared<std::string>("public key4"), {"sig4"}};
            b.metadata.validateSignatures.emplace_back(sig3);
            b.metadata.validateSignatures.emplace_back(sig4);

            proto::SignatureString sig5 = {"ski", std::make_shared<std::string>("public key4"), {"sig4"}};
            std::unique_ptr<proto::Envelop> env1(new proto::Envelop());
            env1->setSignature(std::move(sig5));
            std::string payload("payload for sig5");
            env1->setPayload(std::move(payload));
            b.body.userRequests.push_back(std::move(env1));

            return realBlock;
        }
    };
}