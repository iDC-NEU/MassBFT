//
// Created by user on 24-5-10.
//

#include "peer/chaincode/timeSeries_chaincode.h"
#include "client/timeSeries/timeSeries_helper.h"
#include "client/timeSeries/core_workload.h"
#include "client/core/write_through_db.h"

namespace peer::chaincode {
  using namespace client::timeSeries;

  int TimeSeriesChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
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

  int TimeSeriesChaincode::update(std::string_view argSV) {
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

  int TimeSeriesChaincode::read(std::string_view argSV) {
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

  int TimeSeriesChaincode::remove(std::string_view argSV) {
    LOG(ERROR) << "Method remove is not implemented yet!";
    return -1;
  }

  int TimeSeriesChaincode::scan(std::string_view argSV) {
    LOG(ERROR) << "Method scan is not implemented yet!";
    return -1;
  }

  int TimeSeriesChaincode::readModifyWrite(std::string_view argSV) {
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

  int TimeSeriesChaincode::InitDatabase() {
    ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
    auto* property = util::Properties::GetProperties();
    auto ycsbProperties = TimeSeriesProperties::NewFromProperty(*property);

    // set insert start = 0
    auto oldInsertStart = ycsbProperties->getInsertStart();
    TimeSeriesProperties::SetProperties(TimeSeriesProperties::INSERT_START_PROPERTY, 0);

    // create new workload
    auto workload = std::make_shared<CoreWorkload>();
    workload->init(*property);
    auto measurements = std::make_shared<client::core::Measurements>();
    workload->setMeasurements(measurements);

    // restore the original value
    TimeSeriesProperties::SetProperties(TimeSeriesProperties::INSERT_START_PROPERTY, oldInsertStart);

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

  TimeSeriesChaincode::TimeSeriesChaincode(std::unique_ptr<ORM> orm) : Chaincode(std::move(orm)) {
    auto* property = util::Properties::GetProperties();
    auto ycsbProperties = TimeSeriesProperties::NewFromProperty(*property);
    fieldNames = ycsbProperties->getFieldNames();
  }
}