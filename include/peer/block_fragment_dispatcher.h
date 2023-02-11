//
// Created by peng on 12/1/22.
//

#pragma once

#include "block_fragment_generator.h"
#include "common/bccsp.h"
#include "common/zeromq.h"
#include "proto/block.h"
#include "ankerl/unordered_dense.h"

namespace peer {
    // TODO: FINISH THIS PART
    // Active object.
    // BlockFragmentDispatcher is responsible for:
    // 1. receive fragments of multiple blocks from other peers.
    // 2. assemble fragments into blocks pieces.
    // 3. merge block pieces into block.
    template<class Message=zmq::message_t>
    requires requires(Message m) { static_cast<char*>(m.data()); static_cast<size_t>(m.size()); }
    class BlockFragmentDispatcher {
    public:
        template<class MessageExchangerImpl>
        requires requires(MessageExchangerImpl mei) {
            mei.sendToAddr(std::string_view{}, std::string{});
            mei.receive(std::string_view{}); }
        class MessageExchanger {
        public:
            virtual ~MessageExchanger() = default;

            MessageExchanger(const MessageExchanger&) = delete;

            bool sendToAddr(std::string_view ski, std::string&& message) {
                return impl().doSendToAddr(ski, std::move(message));
            }

            std::optional<Message> receive() {
                return impl().doReceive();
            }

        private:
            MessageExchangerImpl& impl() { return static_cast<MessageExchangerImpl&>(*this); }
            friend MessageExchangerImpl;
        };

        class ZMQMessageExchanger: public MessageExchanger<ZMQMessageExchanger> {
        public:
            ZMQMessageExchanger() = default;

            ZMQMessageExchanger(const ZMQMessageExchanger&) = delete;

        protected:
            bool doSendToAddr(const std::string& ski, std::string&& message) {
                DCHECK(remoteClients.contains(ski));
                return remoteClients[ski]->template send(std::move(message));
            }

            std::optional<Message> doReceive() {
                return localServer->receive();
            }

        private:
            // localServer is responsible for receiving fragments from remote peers.
            std::unique_ptr<util::ZMQInstance> localServer;
            // remoteClients are responsible for sending message fragments to remote peers.
            ankerl::unordered_dense::map<std::string, std::unique_ptr<util::ZMQInstance>> remoteClients;
        };

        struct Config {
            struct RegionInfo {
                // the region ski
                std::string ski{};
                int byzantinePeerCount = 0;
                // a list of peer ski
                std::vector<std::string> peerList{};
                // Instance for parallel processing
                // must be equal for all regions
                int instanceCount = 1;
                // ---- these params below is set by dispatcher ----
            };
            // current peer ski
            std::string ski;
            std::vector<RegionInfo> regionList;
        };
        // use an LRU to cache validated root.
        // Root is validated by local cluster
        inline void addValidatedRoot(const pmt::HashString& root) {

        }

        ~BlockFragmentDispatcher() = default;

        BlockFragmentDispatcher(const BlockFragmentDispatcher&) = delete;

    protected:
        explicit BlockFragmentDispatcher(std::unique_ptr<util::BCCSP> bccsp) {

        }

        // broadcast local block to remote peers.
        // the broadcast operation must be performed in sequence
        bool broadcast(const pmt::HashString& root) {

        }

        // receive some block fragments from remote peers.
        bool receiveEventLoop() {
            while(true) {
                // please cache ret until block generated.
                auto ret = me->receive();
                if (!ret) {
                    LOG(ERROR) << "Receive message fragment failed!";
                    continue;
                }
                std::string_view message(reinterpret_cast<char*>(ret->data()), ret->size());
                zpp::bits::in inEnv(message);

                proto::Envelop env;
                if(failure(inEnv(env))) {
                    LOG(ERROR) << "Decode message fragment failed!";
                    continue;
                }
                proto::EncodeBlockFragment ebf;
                zpp::bits::in inEBF(env.payload);
                if(failure(inEBF(ebf))) {
                    LOG(ERROR) << "Decode message fragment failed!";
                    continue;
                }
                Cache& cache = messageCache[ebf.root];
                // we first check if the root is processing complete
                if (cache.completeFlag.load(std::memory_order_acquire)) {
                    DLOG(INFO) << "We have already generated the root!";
                    continue;
                }
                // preparing to process the actual fragment
                cache.messageList.push_back(std::move(*ret));
                cache.processingThreadCount.fetch_add(1, std::memory_order_relaxed);
                _receiverTP->template push_task([this](const proto::Envelop& env, proto::EncodeBlockFragment ebf, Cache& cache) {
                    // add a thread guard
                    std::shared_ptr<void> defer(nullptr, [&](){ cache.processingThreadCount.fetch_sub(1, std::memory_order_release); });
                    // verify the signature
                    auto* key = _bccsp->GetKey(env.signature.first);
                    if (!key->Verify(env.signature.second, env.payload.data(), env.payload.size())) {
                        LOG(ERROR) << "Verify signature failed!";
                        return;
                    }
                    // validate fragment
                    if (!cache.validator->validateAndDeserializeFragments(ebf.root, ebf.encodeMessage, ebf.start, ebf.end)) {
                        LOG(ERROR) << "Verify fragment failed!";
                        return;
                    }
                    do {
                        auto original = cache.processedFragmentCount.load(std::memory_order_acquire);
                        auto desire = original+ebf.end-ebf.start;
                        if (original >= cache.validator.getDataShardCnt()) {
                            cache.processedFragmentCount.fetch_add(ebf.end-ebf.start, std::memory_order_release);
                            DLOG(INFO) << "early return.";
                            return;
                        }
                        if (!cache.processedFragmentCount.compare_exchange_strong(original, desire, std::memory_order_acquire, std::memory_order_relaxed)) {
                            continue;
                        }
                        // we have collected enough fragment, generate data
                        if (desire >= cache.validator.getDataShardCnt()) {
                            DLOG(INFO) << "mark the func completed.";
                            bool flag = false;
                            cache.completeFlag.compare_exchange_strong(flag, true, std::memory_order_acquire, std::memory_order_relaxed);
                        } else {
                            DLOG(INFO) << "early return.";
                            return;
                        }

                    } while(true);

                }, std::move(env), ebf, cache);
            }
        }

    private:
        // BlockFragmentDispatcher and bfg can share the same thread pool!
        std::unique_ptr<util::thread_pool_light> _tp;
        std::unique_ptr<util::BCCSP> _bccsp;
        std::unique_ptr<peer::BlockFragmentGenerator> _bfg;
        std::unique_ptr<ZMQMessageExchanger> me;
        // cache for receive event loop
        struct Cache {
            std::unique_ptr<peer::BlockFragmentGenerator::Context> validator = nullptr;
            // messageList is NOT thread safe!
            std::vector<std::string> messageList{};
            std::atomic<int> processingThreadCount = 0;
            std::atomic<int> processedFragmentCount = 0;
            std::atomic<bool> completeFlag = false;
            std::atomic<bool> deleteFlag = false;
        };
        // K, V = root, cache
        ankerl::unordered_dense::map<pmt::HashString, Cache> messageCache;
        ankerl::unordered_dense::map<std::string, peer::BlockFragmentGenerator::Config> skiConfigMap;

        std::unique_ptr<util::thread_pool_light> _receiverTP;
    };
}
