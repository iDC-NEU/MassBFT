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
            // funNameSV is short for the operations  eg: r -> read
            if(funcNameSV == "r") {

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

