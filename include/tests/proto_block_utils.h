//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"
#include "common/property.h"

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
            for(int i=0; i<2000; i++) {
                std::unique_ptr<proto::TxReadWriteSet> rwSet(new proto::TxReadWriteSet({"test rw set"}));
                std::unique_ptr<proto::KV> read(new proto::KV("key1", "value1"));
                rwSet->getReads().push_back(std::move(read));
                std::unique_ptr<proto::KV> write(new proto::KV("key2", "value2"));
                rwSet->getWrites().push_back(std::move(write));
                rwSet->setRetCode(1);
                rwSet->setCCNamespace("spec");
                rwSets.push_back(std::move(rwSet));
            }
            b.executeResult.txReadWriteSet = std::move(rwSets);
            b.executeResult.transactionFilter.resize(2);
            b.executeResult.transactionFilter[1] = (std::byte)10;
            proto::SignatureString sig1 = {"ski", 1, {"sig1"}};
            proto::SignatureString sig2 = {"ski", 2, {"sig2"}};
            b.metadata.consensusSignatures.emplace_back("", sig1);
            b.metadata.consensusSignatures.emplace_back("", sig2);

            proto::SignatureString sig3 = {"ski", 3, {"sig3"}};
            proto::SignatureString sig4 = {"ski", 4, {"sig4"}};
            b.metadata.validateSignatures.emplace_back("", sig3);
            b.metadata.validateSignatures.emplace_back("", sig4);

            proto::SignatureString sig5 = {"ski", 5, {"sig4"}};
            std::unique_ptr<proto::Envelop> env1(new proto::Envelop());
            env1->setSignature(std::move(sig5));
            std::string payload("payload for sig5");
            env1->setPayload(std::move(payload));
            b.body.userRequests.push_back(std::move(env1));

            return realBlock;
        }

        static std::vector<std::shared_ptr<util::ZMQInstanceConfig>> GenerateNodesConfig(int groupId, int count, int portOffset) {
            std::vector<std::shared_ptr<util::ZMQInstanceConfig>> nodesConfig;
            for (int i = 0; i < count; i++) {
                auto cfg = std::make_shared<util::ZMQInstanceConfig>();
                auto nodeCfg = std::make_shared<util::NodeConfig>();
                nodeCfg->groupId = groupId;
                nodeCfg->nodeId = i;
                nodeCfg->priIp = "127.0.0.1";
                nodeCfg->pubIp = "127.0.0.1";
                nodeCfg->ski = std::to_string(groupId) + "_" + std::to_string(i);
                cfg->nodeConfig = std::move(nodeCfg);
                cfg->port = 51200 + portOffset + i;
                nodesConfig.push_back(std::move(cfg));
            }
            return nodesConfig;
        }

        static std::vector<std::shared_ptr<util::NodeConfig>> GenerateNodesConfig(int groupId, int count) {
            std::vector<std::shared_ptr<util::NodeConfig>> nodesConfig;
            for (int i = 0; i < count; i++) {
                auto nodeCfg = std::make_shared<util::NodeConfig>();
                nodeCfg->groupId = groupId;
                nodeCfg->nodeId = i;
                nodeCfg->priIp = "127.0.0.1";
                nodeCfg->pubIp = "127.0.0.1";
                nodeCfg->ski = std::to_string(groupId) + "_" + std::to_string(i);
                nodesConfig.push_back(std::move(nodeCfg));
            }
            return nodesConfig;
        }

        static std::unique_ptr<proto::Envelop> CreateMockEnvelop(int nonce = 0) {
            proto::SignatureString sig5 = {"ski" + std::to_string(nonce), nonce, {"sig"}};
            std::unique_ptr<proto::Envelop> envelop(new proto::Envelop());
            envelop->setSignature(std::move(sig5));
            std::string payload("payload for sig" + std::to_string(nonce));
            envelop->setPayload(std::move(payload));
            return envelop;
        }
    };
}