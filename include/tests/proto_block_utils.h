//
// Created by peng on 2/18/23.
//

#pragma once

#include "proto/block.h"
#include "common/property.h"

#include <chrono>

namespace tests {
    class ProtoBlockUtils {
    public:
        static std::unique_ptr<proto::Block> CreateDemoBlock() {
            std::unique_ptr<proto::Block> realBlock(new proto::Block);
            proto::Block& b = *realBlock;
            auto now = std::chrono::system_clock::now();
            auto duration = now.time_since_epoch();
            long unix_timestamp_value = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
            std::string unix_timestamp = std::to_string(unix_timestamp_value);

            b.header.dataHash = {"dataHash"};
            b.header.previousHash = {"previousHash"};
            b.header.number = 10;
            b.header.timeStamp = unix_timestamp;
            std::vector<std::unique_ptr<proto::TxReadWriteSet>> rwSets;
            for(int i=0; i<2000; i++) {
                std::unique_ptr<proto::TxReadWriteSet> rwSet(new proto::TxReadWriteSet({"test rw set"}));
                std::unique_ptr<proto::KV> read(new proto::KV("key1", "value1"));
                rwSet->getReads().push_back(std::move(read));
                std::unique_ptr<proto::KV> write(new proto::KV("key2", "value2"));
                rwSet->getWrites().push_back(std::move(write));
                rwSet->setRetValue("return value of the tx");
                rwSet->setRetCode(1);
                rwSets.push_back(std::move(rwSet));
            }
            b.executeResult.txReadWriteSet = std::move(rwSets);
            b.executeResult.transactionFilter.resize(2000);
            b.executeResult.transactionFilter[1] = (std::byte)10;
            proto::SignatureString sig1 = {"ski", {"sig1"}};
            proto::SignatureString sig2 = {"ski", {"sig2"}};
            b.metadata.consensusSignatures.emplace_back("", sig1);
            b.metadata.consensusSignatures.emplace_back("", sig2);

            proto::SignatureString sig3 = {"ski", {"sig3"}};
            proto::SignatureString sig4 = {"ski", {"sig4"}};
            b.metadata.validateSignatures.emplace_back("", sig3);
            b.metadata.validateSignatures.emplace_back("", sig4);

            for (int i=0; i<10; i++) {
                proto::SignatureString sig5 = {"ski", {"sig4"}};
                std::unique_ptr<proto::Envelop> env1(new proto::Envelop());
                env1->setSignature(std::move(sig5));
                std::string payload("payload for sig" + std::to_string(i));
                env1->setPayload(std::move(payload));
                b.body.userRequests.push_back(std::move(env1));
            }
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
            proto::SignatureString sig5 = {"ski" + std::to_string(nonce), {"sig"}};
            std::unique_ptr<proto::Envelop> envelop(new proto::Envelop());
            envelop->setSignature(std::move(sig5));
            std::string payload("payload for sig" + std::to_string(nonce));
            envelop->setPayload(std::move(payload));
            return envelop;
        }
    };
}