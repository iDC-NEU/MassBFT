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
            if (funcNameSV == "Get") {
                zpp::bits::in in(argSV);
                std::string_view key;
                if (failure(in(key))) {
                    return -1;
                }
                return Get(key);
            }
            if(funcNameSV == "Set") {
                zpp::bits::in in(argSV);
                std::string_view key, value;
                if (failure(in(key, value))) {
                    return -1;
                }
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
                orm->setResult(value);
                return 0;
            }
            return -1;
        }
    };
}