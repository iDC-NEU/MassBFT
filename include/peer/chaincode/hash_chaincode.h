//
// Created by user on 23-7-12.
//

#pragma once

#include "peer/chaincode/chaincode.h"

namespace peer::chaincode {
    class HashChaincode : public Chaincode {
    public:
        explicit HashChaincode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override {
            std::vector<std::string_view> args;
            zpp::bits::in in(argSV);
            if (failure(in(args))) {
                LOG(WARNING) << "Chaincode args deserialize failed!";
                return -1;
            }
            if (args.size() != 2 && args.size() != 1) {
                return -1;
            }
            auto& key = args[0];
            auto& value = args[1];
            // If the key is used as a distinction, the read and write sets are disjoint
            if (funcNameSV == "Get") {
                return Get(key);
            }
            if(funcNameSV == "Set") {
                return Set(key, value);
            }
            return -1;
        }

        int Set(std::string_view key, std::string_view value) {
            orm->put(std::string(key), std::string(value));
            return 0;
        }

        int Get(std::string_view key) {
            std::string_view value;
            if (orm->get(std::string(key), &value)) {
                return 0;
            }
            return -1;
        }
    };
}