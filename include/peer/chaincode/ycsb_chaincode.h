//
// Created by user on 23-6-30.
//

#pragma once

#include "chaincode.h"

namespace peer::chaincode {
    class YCSBChainCode : public Chaincode {
    public:
        explicit YCSBChainCode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, const std::vector<std::string_view>& args) override {
            if(funcNameSV == "init") {
                InitDatabase(std::stoi(args[0].data()));
            }
            // funNameSV is short for the operations  eg: r -> read
            std::string table;
            std::string key;
            std::vector<std::string> fields;
            std::vector<std::pair<std::string, std::string>> writeArgs;

            zpp::bits::in in(args[0]);
            if (failure(in(table, key))) {
                return -1;
            }

            if(funcNameSV == "r") {
                if(failure(in(fields))) {
                    return -1;
                }
                return Get(std::string_view(key));
            }
            if(funcNameSV == "i") {
                if(failure(in(writeArgs))){
                    return -1;
                }
                return Set(std::string_view(key), "");
            }
            if(funcNameSV == "u") {
                if(failure(in(writeArgs))){
                    return -1;
                }
                return Set(std::string_view(key), "");
            }
            if(funcNameSV == "d") {
                return Set(std::string_view(key), "");
            }
            if(funcNameSV == "s") {
                // do not support scan op
                return -1;
            }
            if(funcNameSV == "rmw") {
                if(failure(in(fields, writeArgs))){
                    return -1;
                }
                if(Get(key) == -1) {
                    return -1;
                }
                return Set(std::string_view(key), "");
            }
            return 0;
        }

        int InitDatabase(int recordCount) {
            LOG(INFO) << "Start initializing data: " << recordCount;
            for (int i=0; i<recordCount; i++) {
                orm->put(std::to_string(i), "0");
            }
            return 0;
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

