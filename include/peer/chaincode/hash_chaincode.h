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
            if (funcNameSV == "GetHistory") {
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
            std::vector<std::string_view> valueList;
            std::string_view oldValueRaw;
            if (orm->get(std::string(key), &oldValueRaw)) {
                zpp::bits::in in(oldValueRaw);
                if (failure(in(valueList))) {
                    // deserialize failed
                    return -1;
                }
            }
            valueList.emplace_back(value);
            std::string newValueRaw;
            zpp::bits::out out(newValueRaw);
            if(failure(out(valueList))) {
                // serialize failed
                return -1;
            }
            orm->put(std::string(key), std::move(newValueRaw));
            return 0;
        }
        int Get(std::string_view key) {
            std::vector<std::string_view> valueList;
            std::string_view oldValueRaw;
            if (orm->get(std::string(key), &oldValueRaw)) {
                zpp::bits::in in(oldValueRaw);
                if (failure(in(valueList))) {
                    // deserialize failed
                    return -1;
                }
                std::string_view value = valueList.back();
                orm->setResult(value);
                return 0;
            }
            return -1;
        }
        int GetHistory(std::string_view key) {
            std::vector<std::string_view> valueList;
            std::string_view oldValueRaw;
            if (orm->get(std::string(key), &oldValueRaw)) {
                zpp::bits::in in(oldValueRaw);
                if (failure(in(valueList))) {
                    // deserialize failed
                    return -1;
                }
                orm->setResult(oldValueRaw);
                return 0;
            }
            return -1;
        }
    };
}