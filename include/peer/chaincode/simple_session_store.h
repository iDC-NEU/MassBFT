//
// Created by user on 23-3-7.
//

#pragma once


#include "peer/chaincode/chaincode.h"

namespace peer::chaincode {
    class SimpleSessionStore : public Chaincode {
    public:
        explicit SimpleSessionStore(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override {
            std::vector<std::string_view> args;
            zpp::bits::in in(argSV);
            if (failure(in(args))) {
                LOG(WARNING) << "Chaincode args deserialize failed!";
                return -1;
            }
            if (args.size() == 1 && funcNameSV == "init") {
                return initDatabase(std::stoi(std::string(args[0])));
            }
            if (args.size() != 2) {
                return -1;
            }
            auto& key = args[0];
            auto& value = args[1];
            // If the key is used as a distinction, the read and write sets are disjoint
            auto valueNum = std::atoi(value.data());
            if (valueNum%2 == 0) {
                return Get(key);
            } else {
                return Set(key, value);
            }
        }

        int initDatabase(int recordCount) {
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