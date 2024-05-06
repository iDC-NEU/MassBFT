//
// Created by user on 24-4-16.
//

#pragma once

#include "peer/storage/mr_block_storage.h"
#include "common/zeromq.h"
#include "common/property.h"
#include "proto/block.h"

namespace peer::multiway {
    class MRBlockSender {
    public:
        MRBlockSender(const MRBlockSender&) = delete;

        MRBlockSender(MRBlockSender&&) = delete;

        using ConfigPtr = std::shared_ptr<util::ZMQInstanceConfig>;

        static std::unique_ptr<MRBlockSender> NewMRBlockSender(
                const std::unordered_map<int, std::vector<ConfigPtr>>& regionConfig,
                int groupId,
                int nodeId) {
            std::unique_ptr<MRBlockSender> mrBlockSender(new MRBlockSender);
            mrBlockSender->_groupId = groupId;
            for (const auto& it: regionConfig) {
                if (it.first == groupId) {
                    continue; // skip local group
                }
                // send to node id localRegionId in each group if node id exists
                if ((int)it.second.size() < nodeId + 1) {
                    continue;
                }
                mrBlockSender->_receivers.push_back(it.second[nodeId]);
                CHECK(it.second[nodeId]->nodeConfig->nodeId == nodeId);
                LOG(INFO) << "My group id: " << groupId << " send to group: " << it.first << ", id: " << nodeId;
            }

            for (const auto& it: mrBlockSender->_receivers) {
                auto zmqClient = util::ZMQInstance::NewClient<zmq::socket_type::pub>(it->pubAddr(), it->port);
                if (zmqClient == nullptr) {
                    LOG(ERROR) << "Can not create ZMQClient!";
                    return nullptr;
                }
                mrBlockSender->_senders.push_back(std::move(zmqClient));
            }
            return mrBlockSender;
        }

        virtual ~MRBlockSender() {
            _tearDownSignal = true;
            if (_thread) {
                _thread->join();
            }
        }

        void setStorage(std::shared_ptr<MRBlockStorage> storage) { _storage = std::move(storage); }

        bool checkAndStart(int startFromBlock) {
            _thread = std::make_unique<std::thread>(&MRBlockSender::run, this, startFromBlock);
            return true;
        }

    protected:
        MRBlockSender() = default;

        void run(int startFromBlock) {
            pthread_setname_np(pthread_self(), "blk_sender");
            auto nextBlockNumber = startFromBlock;
            LOG(INFO) << "BlockSender start from block: " << nextBlockNumber;
            while (!_tearDownSignal) {
                auto block = _storage->waitForBlock(_groupId, nextBlockNumber, 1000);
                if (block == nullptr) {
                    continue;   // unexpected wakeup
                }
                CHECK(!block->metadata.consensusSignatures.empty()) << "Block have not go through consensus!";
                std::string serializedBlock;
                auto success = block->serializeToString(&serializedBlock);
                if (!success.valid) {
                    LOG(INFO) << "Serialize block failed, block number: " << block->header.number;
                    nextBlockNumber++;
                    continue;
                }
                const std::string& message = serializedBlock;
                // LOG(INFO) << "Sender send a block, size: " <<block->body.userRequests.size()
                //           << ", RawSize:" << serializedBlock.size()
                //           << ", idx:" << block->header.number
                //           << ", receivers:" << _senders.size();
                for (auto& it: _senders) {
                    if (!it->send(message)) {
                        LOG(ERROR) << "blk_sender can not send block";
                    }
                }
                nextBlockNumber++;
            }
        }

    private:
        // signal to alert if the system is shutdown
        volatile bool _tearDownSignal = false;
        std::unique_ptr<std::thread> _thread;
        int _groupId = -1;
        // shared storage
        std::shared_ptr<MRBlockStorage> _storage;
        std::vector<ConfigPtr> _receivers;
        std::vector<std::unique_ptr<util::ZMQInstance>> _senders;
    };

}