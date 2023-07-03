//
// Created by user on 23-7-3.
//

#include "peer/chaincode/ycsb_chaincode.h"
#include "ycsb/core/common/ycsb_property.h"
#include "ycsb/core/workload/core_workload.h"

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
            orm->put(BuildKeyField(key, it.first), std::string(it.second));
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
            if (!orm->get(BuildKeyField(key, it), &value)) {
                return -1;
            }
        }
        for (const auto& it: writeArgs) {
            orm->put(BuildKeyField(key, it.first), std::string(it.second));
        }
        return 0;
    }


    class WriteThroughDB: public ycsb::core::DB {
    public:
        explicit WriteThroughDB(ORM* orm) :_orm(orm) { }

        void stop() override { }

        ycsb::core::Status read(const std::string&, const std::string&, const std::vector<std::string>&) override {
            return ycsb::core::ERROR;
        }

        ycsb::core::Status scan(const std::string&, const std::string&, uint64_t, const std::vector<std::string>&) override {
            return ycsb::core::ERROR;
        }

        ycsb::core::Status update(const std::string&, const std::string&, const ycsb::utils::ByteIteratorMap&) override {
            return ycsb::core::ERROR;
        }

        ycsb::core::Status readModifyWrite(const std::string&, const std::string&, const std::vector<std::string>&, const ycsb::utils::ByteIteratorMap&) override {
            return ycsb::core::ERROR;
        }

        ycsb::core::Status insert(const std::string&, const std::string& key, const ycsb::utils::ByteIteratorMap& values) override {
            for (const auto& it: values) {
                _orm->put(YCSBChainCode::BuildKeyField(key, it.first), std::string(it.second));
            }
            return ycsb::core::STATUS_OK;
        }

        ycsb::core::Status remove(const std::string&, const std::string&) override { return ycsb::core::ERROR; }

    private:
        ORM* _orm;
    };

    int YCSBChainCode::InitDatabase() {
        ::ycsb::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto ycsbProperties = ycsb::utils::YCSBProperties::NewFromProperty(*property);

        // set insert start = 0
        auto oldInsertStart = ycsbProperties->getInsertStart();
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::INSERT_START_PROPERTY, 0);

        // create new workload
        auto workload = std::make_shared<ycsb::core::workload::CoreWorkload>();
        workload->init(*ycsbProperties);
        auto measurements = std::make_shared<ycsb::core::Measurements>();
        workload->setMeasurements(measurements);

        // restore the original value
        ycsb::utils::YCSBProperties::SetYCSBProperties(ycsb::utils::YCSBProperties::INSERT_START_PROPERTY, oldInsertStart);

        // start load data
        auto opCount = ycsbProperties->getRecordCount();
        auto db = std::make_unique<WriteThroughDB>(orm.get());
        for (auto i=0; i<opCount; i++) {
            if (!workload->doInsert(db.get())) {
                LOG(ERROR) << "Load data failed!";
                return -1;
            }
        }
        return 0;
    }

    YCSBChainCode::YCSBChainCode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) {
        auto* property = util::Properties::GetProperties();
        auto ycsbProperties = ycsb::utils::YCSBProperties::NewFromProperty(*property);
        fieldNames = ycsbProperties->getFieldNames();
    }
}