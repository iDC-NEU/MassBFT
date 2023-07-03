//
// Created by user on 23-7-3.
//

#include "peer/chaincode/ycsb_chaincode.h"

namespace peer::chaincode {

    int YCSBChainCode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        switch (funcNameSV[0]) {
            case 'u':   // update op
                return this->update(argSV);
            case 'i':   // insert op
                return this->insert(argSV);
            case 'r':   // read op
                return this->read(argSV);
            case 'd':   // delete op
                return this->remove(argSV);
            case 's':   // scan op
                return this->scan(argSV);
            case 'm':   // modify op
                return this->readModifyWrite(argSV);
            default:
                LOG(ERROR) << "Unknown method!";
                return -1;
        }
    }

    int YCSBChainCode::update(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::pair<std::string_view, std::string_view>> args;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, args))) {
            return -1;
        }
        for (const auto& it: args) {
            orm->put(buildKeyField(key, it.first), std::string(it.second));
        }
        return 0;
    }

    int YCSBChainCode::read(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> fields;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, fields))) {
            return -1;
        }
        for (const auto& it: fields) {
            std::string_view value; // drop the value
            if (!orm->get(buildKeyField(key, it), &value)) {
                return -1;
            }
        }
        return 0;
    }

    int YCSBChainCode::remove(std::string_view argSV) {
        LOG(ERROR) << "Method remove is not implemented yet!";
        return -1;
    }

    int YCSBChainCode::scan(std::string_view argSV) {
        LOG(ERROR) << "Method scan is not implemented yet!";
        return -1;
    }

    int YCSBChainCode::readModifyWrite(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> readFields;
        std::vector<std::pair<std::string_view, std::string_view>> writeArgs;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, readFields, writeArgs))) {
            return -1;
        }
        for (const auto& it: readFields) {
            std::string_view value; // drop the value
            if (!orm->get(buildKeyField(key, it), &value)) {
                return -1;
            }
        }
        for (const auto& it: writeArgs) {
            orm->put(buildKeyField(key, it.first), std::string(it.second));
        }
        return 0;
    }

    int YCSBChainCode::InitDatabase(int recordCount) {
        LOG(ERROR) << "Method InitDatabase is not implemented yet!";
        return -1;
    }
}