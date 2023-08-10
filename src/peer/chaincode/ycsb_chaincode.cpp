//
// Created by user on 23-7-3.
//

#include "peer/chaincode/ycsb_chaincode.h"
#include "client/ycsb/ycsb_property.h"
#include "client/ycsb/core_workload.h"
#include "client/core/write_through_db.h"

namespace peer::chaincode {
    int YCSBChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
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

    int YCSBChaincode::update(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::pair<std::string_view, std::string_view>> args;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, args))) {
            return -1;
        }
        for (const auto& it: args) {
            orm->put(BuildKeyField(key, it.first), std::string(it.second));
        }
        return 0;
    }

    int YCSBChaincode::read(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> fields;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, fields))) {
            return -1;
        }
        if (fields.empty()) {
            for (const auto& it: fieldNames) {
                std::string_view value; // drop the value
                if (!orm->get(BuildKeyField(key, it), &value)) {
                    return -1;
                }
            }
        } else {
            for (const auto& it: fields) {
                std::string_view value; // drop the value
                if (!orm->get(BuildKeyField(key, it), &value)) {
                    return -1;
                }
            }
        }
        return 0;
    }

    int YCSBChaincode::remove(std::string_view argSV) {
        LOG(ERROR) << "Method remove is not implemented yet!";
        return -1;
    }

    int YCSBChaincode::scan(std::string_view argSV) {
        LOG(ERROR) << "Method scan is not implemented yet!";
        return -1;
    }

    int YCSBChaincode::readModifyWrite(std::string_view argSV) {
        std::string_view table, key;
        std::vector<std::string_view> readFields;
        std::vector<std::pair<std::string_view, std::string_view>> writeArgs;
        zpp::bits::in in(argSV);
        if(failure(in(table, key, readFields, writeArgs))) {
            return -1;
        }
        if (readFields.empty()) {
            for (const auto& it: fieldNames) {
                std::string_view value; // drop the value
                if (!orm->get(BuildKeyField(key, it), &value)) {
                    return -1;
                }
            }
        } else {
            for (const auto& it: readFields) {
                std::string_view value; // drop the value
                if (!orm->get(BuildKeyField(key, it), &value)) {
                    return -1;
                }
            }
        }
        for (const auto& it: writeArgs) {
            orm->put(BuildKeyField(key, it.first), std::string(it.second));
        }
        return 0;
    }

    int YCSBChaincode::InitDatabase() {
        ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto ycsbProperties = client::ycsb::YCSBProperties::NewFromProperty(*property);

        // set insert start = 0
        auto oldInsertStart = ycsbProperties->getInsertStart();
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::INSERT_START_PROPERTY, 0);

        // create new workload
        auto workload = std::make_shared<client::ycsb::CoreWorkload>();
        workload->init(*property);
        auto measurements = std::make_shared<client::core::Measurements>();
        workload->setMeasurements(measurements);

        // restore the original value
        client::ycsb::YCSBProperties::SetYCSBProperties(client::ycsb::YCSBProperties::INSERT_START_PROPERTY, oldInsertStart);

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

    YCSBChaincode::YCSBChaincode(std::unique_ptr<ORM> orm) : Chaincode(std::move(orm)) {
        auto* property = util::Properties::GetProperties();
        auto ycsbProperties = client::ycsb::YCSBProperties::NewFromProperty(*property);
        fieldNames = ycsbProperties->getFieldNames();
    }
}