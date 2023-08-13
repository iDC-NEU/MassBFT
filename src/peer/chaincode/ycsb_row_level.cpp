//
// Created by user on 23-7-3.
//
#include "peer/chaincode/ycsb_row_level.h"
#include "client/ycsb/ycsb_helper.h"
#include "client/ycsb/core_workload.h"
#include "client/core/write_through_db.h"

namespace peer::chaincode {
    using namespace client::ycsb;

    int chaincode::YCSBRowLevel::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        switch (funcNameSV[0]) {
            case InvokeRequestType::UPDATE[0]:   // update op
                return this->update(argSV);
            case InvokeRequestType::INSERT[0]:   // insert op
                return this->insert(argSV);
            case InvokeRequestType::READ[0]:   // read op
                return this->read(argSV);
            case InvokeRequestType::DELETE[0]:   // delete op
                return this->remove(argSV);
            case InvokeRequestType::SCAN[0]:   // scan op
                return this->scan(argSV);
            case InvokeRequestType::READ_MODIFY_WRITE[0]:   // modify op
                return this->readModifyWrite(argSV);
            default:
                LOG(ERROR) << "Unknown method!";
                return -1;
        }
    }

    int peer::chaincode::YCSBRowLevel::InitDatabase() {
        ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto ycsbProperties = YCSBProperties::NewFromProperty(*property);

        // set insert start = 0
        auto oldInsertStart = ycsbProperties->getInsertStart();
        YCSBProperties::SetProperties(YCSBProperties::INSERT_START_PROPERTY, 0);

        // create new workload
        auto workload = std::make_shared<CoreWorkload>();
        workload->init(*property);
        auto measurements = std::make_shared<client::core::Measurements>();
        workload->setMeasurements(measurements);

        // restore the original value
        YCSBProperties::SetProperties(YCSBProperties::INSERT_START_PROPERTY, oldInsertStart);

        // start load data
        auto opCount = ycsbProperties->getRecordCount();
        auto db = std::make_unique<client::core::WriteThroughDB>(this);
        for (auto i=0; i<opCount; i++) {
            if (!workload->doInsert(db.get())) {
                LOG(ERROR) << "Load data failed!";
                return -1;
            }
        }
        return 0;
    }

    int YCSBRowLevel::update(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::pair<std::string_view, std::string_view>> args;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, args))) {
            return -1;
        }
        return updateValue(key, args);
    }

    int YCSBRowLevel::read(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> fields;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, fields))) {
            return -1;
        }
        std::string_view rawReadValue;
        if (!orm->get(std::string(key), &rawReadValue)) {
            return -1;
        }
        return 0;
    }

    int YCSBRowLevel::insert(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::pair<std::string_view, std::string_view>> rhs;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, rhs))) {
            return -1;
        }
        return insertValue(key, rhs);
    }

    int YCSBRowLevel::remove(std::string_view argSV) {
        LOG(ERROR) << "Method remove is not implemented yet!";
        return -1;
    }

    int YCSBRowLevel::scan(std::string_view argSV) {
        LOG(ERROR) << "Method scan is not implemented yet!";
        return -1;
    }

    int YCSBRowLevel::readModifyWrite(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> readFields;
        std::vector<std::pair<std::string_view, std::string_view>> writeArgs;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, readFields, writeArgs))) {
            return -1;
        }
        return modifyValue(key, readFields, writeArgs);
    }

    int YCSBRowLevel::updateValue(std::string_view keySV, const std::vector<std::pair<std::string_view, std::string_view>> &rhs) {
        std::string_view rawReadValue;
        if (!orm->get(std::string(keySV), &rawReadValue)) {
            return -1;
        }
        // deserialize original value
        zpp::bits::in in(rawReadValue);
        std::unordered_map<std::string_view, std::string_view> lhs;
        if(failure(in(lhs))) {
            return -1;
        }
        // merge
        for (const auto& it: rhs) {
            lhs[it.first] = it.second;
        }
        // serialize final value
        std::string rawWriteValue;
        zpp::bits::out out(rawWriteValue);
        if(failure(out(lhs))) {
            return -1;
        }
        orm->put(std::string(keySV), std::move(rawWriteValue));
        return 0;
    }

    int YCSBRowLevel::modifyValue(std::string_view keySV, const std::vector<std::string_view>& reads,
                                  const std::vector<std::pair<std::string_view, std::string_view>> &rhs) {
        std::string_view rawReadValue;
        if (!orm->get(std::string(keySV), &rawReadValue)) {
            return -1;
        }
        // deserialize original value
        zpp::bits::in in(rawReadValue);
        std::unordered_map<std::string_view, std::string_view> lhs;
        if(failure(in(lhs))) {
            return -1;
        }
        // read
        for (const auto& it: reads) {
            if (!lhs.contains(it)) {
                return -1;  // read not found
            }
        }
        // update
        for (const auto& it: rhs) {
            lhs[it.first] = it.second;
        }
        // serialize final value
        std::string rawWriteValue;
        zpp::bits::out out(rawWriteValue);
        if(failure(out(lhs))) {
            return -1;
        }
        orm->put(std::string(keySV), std::move(rawWriteValue));
        return 0;
    }

    int YCSBRowLevel::insertValue(std::string_view keySV, const std::vector<std::pair<std::string_view, std::string_view>> &rhs) {
        std::string_view rawReadValue;
        std::unordered_map<std::string_view, std::string_view> lhs;
        if (orm->get(std::string(keySV), &rawReadValue)) {
            // deserialize original value (if found)
            zpp::bits::in in(rawReadValue);
            if(failure(in(lhs))) {
                return -1;
            }
        }
        // merge (if not exist)
        for (const auto& it: rhs) {
            if (!lhs.insert({it.first, it.second}).second) {
                return -1;
            }
        }
        // serialize final value
        std::string rawWriteValue;
        zpp::bits::out out(rawWriteValue);
        if(failure(out(lhs))) {
            return -1;
        }
        orm->put(std::string(keySV), std::move(rawWriteValue));
        return 0;
    }

}

