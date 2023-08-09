//
// Created by user on 23-7-3.
//
#include "peer/chaincode/ycsb_row_level.h"
#include "client/ycsb/ycsb_property.h"
#include "client/ycsb/core_workload.h"

namespace peer::chaincode {
    int chaincode::YCSBRowLevel::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
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

    class WriteThroughDB: public client::core::DB {
    public:
        explicit WriteThroughDB(ORM* orm) :_orm(orm) { }

        void stop() override { }

        client::core::Status read(const std::string&, const std::string&, const std::vector<std::string>&) override {
            return client::core::ERROR;
        }

        client::core::Status scan(const std::string&, const std::string&, uint64_t, const std::vector<std::string>&) override {
            return client::core::ERROR;
        }

        client::core::Status update(const std::string&, const std::string&, const client::utils::ByteIteratorMap&) override {
            return client::core::ERROR;
        }

        client::core::Status readModifyWrite(const std::string&, const std::string&, const std::vector<std::string>&, const client::utils::ByteIteratorMap&) override {
            return client::core::ERROR;
        }

        client::core::Status insert(const std::string&, const std::string& key, const client::utils::ByteIteratorMap& rhs) override {
            std::unordered_map<std::string_view, std::string_view> lhs;
            for (const auto& it: rhs) {
                lhs[it.first] = it.second;
            }
            // serialize final value
            std::string rawWriteValue;
            zpp::bits::out out(rawWriteValue);
            if(failure(out(lhs))) {
                return client::core::ERROR;
            }
            _orm->put(std::string(key), std::move(rawWriteValue));
            return client::core::STATUS_OK;
        }

        client::core::Status remove(const std::string&, const std::string&) override { return client::core::ERROR; }

    private:
        ORM* _orm;
    };

    int peer::chaincode::YCSBRowLevel::InitDatabase() {
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
        auto db = std::make_unique<WriteThroughDB>(orm.get());
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

