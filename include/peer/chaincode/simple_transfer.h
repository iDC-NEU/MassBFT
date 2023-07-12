//
// Created by peng on 2/20/23.
//

#pragma once

#include "peer/chaincode/chaincode.h"

namespace peer::chaincode {
    class SimpleTransfer : public Chaincode {
    public:
        explicit SimpleTransfer(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        // return ret code
        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override {
            std::vector<std::string_view> args;
            zpp::bits::in in(argSV);
            if (failure(in(args))) {
                LOG(WARNING) << "Chaincode args deserialize failed!";
                return -1;
            }
            if (args.size() != 2) {
                // init data here
                if (funcNameSV == "init" && args.size() == 1) {
                    return initDatabase(std::stoi(std::string(args[0])));
                }
                return -1;
            }
            auto& from = args[0];
            auto& to = args[1];
            if (from == to) {
                return 0;   // optimize: transfer money to self
            }
            std::string_view fromBalance;
            std::string_view toBalance;
            if (!orm->get(std::string(from), &fromBalance) || !orm->get(std::string(to), &toBalance)) {
                LOG(WARNING) << "Can not get value!";
                return -1;
            }
            int mfb = std::atoi(fromBalance.data()) - 100;
            int mtb = std::atoi(toBalance.data()) + 100;
            orm->put(std::string(from), std::to_string(mfb));
            orm->put(std::string(to), std::to_string(mtb));
            return 0;
        }

        int initDatabase(int recordCount) {
            LOG(INFO) << "Start initializing data: " << recordCount;
            for (int i=0; i<recordCount; i++) {
                orm->put(std::to_string(i), "0");
            }
            return 0;
        }

    };
}