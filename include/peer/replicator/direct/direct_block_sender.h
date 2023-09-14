//
// Created by user on 23-9-9.
//

#pragma once

#include "peer/storage/mr_block_storage.h"
#include "common/zeromq.h"
#include "common/property.h"
#include "proto/block.h"

namespace peer::direct {
    // MRBlockSender is responsible for sending blocks across domains to different "regions" (as a node)
    class MRBlockSender {
    public:
        MRBlockSender(const MRBlockSender&) = delete;

        MRBlockSender(MRBlockSender&&) = delete;

        using ConfigPtr = std::shared_ptr<util::ZMQInstanceConfig>;

        static std::unique_ptr<MRBlockSender> NewMRBlockSender(
                // key: region id; value: nodes zmq config
                const std::unordered_map<int, std::vector<ConfigPtr>>& regionConfig,
                int localRegionId) {
            CHECK(regionConfig.contains(localRegionId)) << "allNodesList input error!";
            std::unique_ptr<MRBlockSender> mrBlockSender(new MRBlockSender);
            mrBlockSender->_localRegionId = localRegionId;
            for (const auto& it: regionConfig) {
                // skip local region
                if (it.first == localRegionId) {
                    continue;
                }
                // calculate f + 1 nodes, remoteRegionConfig=it.second
                auto f = ((int)it.second.size() - 1) / 3;
                DLOG(INFO) << "Regions: " << it.first << ", size: " << it.second.size() << ", f: " << f;
                CHECK(f + 1 >= 1 && f + 1 <= (int)it.second.size());
                for (int i = 0; i < f + 1; i += 1) {
                    mrBlockSender->_receivers.push_back(it.second[i]);
                }
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
                auto block = _storage->waitForBlock(_localRegionId, nextBlockNumber, 1000);
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
        int _localRegionId = -1;
        // shared storage
        std::shared_ptr<MRBlockStorage> _storage;
        std::vector<ConfigPtr> _receivers;
        std::vector<std::unique_ptr<util::ZMQInstance>> _senders;
    };

}