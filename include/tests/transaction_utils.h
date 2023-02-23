//
// Created by peng on 2/23/23.
//

#pragma once

#include "proto/transaction.h"
#include <random>

namespace tests {
    class TransactionUtils {
    public:
        static auto CreateRandomKVs(int keySize=10, int range=10000) {
            std::random_device rd;
            std::default_random_engine rng(rd());
            std::uniform_int_distribution<> dist(0, range-1);

            proto::KVList reads;
            proto::KVList writes;
            for (int i=0; i<keySize; i++) {
                reads.push_back(std::make_unique<proto::KV>(std::to_string(dist(rng)), "value"));
                writes.push_back(std::make_unique<proto::KV>(std::to_string(dist(rng)), "value"));
            }
            return std::make_pair(std::move(reads), std::move(writes));
        }

        static void CreateMockTxn(std::vector<std::unique_ptr<proto::Transaction>>* txnList, int count, int range) {
            txnList->clear();
            txnList->reserve(count);
            auto envelopList = CreateMockEnvelop(count, range);
            for(int i=0; i<count; i++) {
                auto txn = proto::Transaction::NewTransactionFromEnvelop(std::move(envelopList[i]));
                txnList->push_back(std::move(txn));
            }
        }

        static std::string ParamToString(const std::vector<std::string>& args) {
            std::string argRaw;
            zpp::bits::out out(argRaw);
            CHECK(!failure(out(args)));
            return argRaw;
        }

        static std::vector<std::unique_ptr<proto::Envelop>> CreateMockEnvelop(int count, int range) {
            // init random
            std::random_device rd;
            std::default_random_engine rng(rd());
            std::uniform_int_distribution<> dist(0, range-1);

            std::vector<std::unique_ptr<proto::Envelop>> envelopList;
            envelopList.reserve(count);
            for (int i=0; i<count; i++) {
                proto::UserRequest request;
                // set from and to
                request.setArgs(ParamToString({std::to_string(dist(rng)), std::to_string(dist(rng))}));
                // serialize
                std::string requestRaw;
                zpp::bits::out out(requestRaw);
                CHECK(!failure(out(request)));
                // wrap it
                std::unique_ptr<proto::Envelop> envelop(new proto::Envelop);
                envelop->setPayload(std::move(requestRaw));
                // compute tid
                proto::SignatureString signature;
                auto digest = std::to_string(i);
                std::copy(digest.begin(), digest.end(), signature.digest.data());
                envelop->setSignature(std::move(signature));
                envelopList.push_back(std::move(envelop));
            }
            return envelopList;
        }

    };
}