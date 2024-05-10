//
// Created by user on 24-5-10.
//

#pragma once

#include "peer/chaincode/chaincode.h"

namespace peer::chaincode {
  class TimeSeriesChaincode : public Chaincode {
  public:
    explicit TimeSeriesChaincode(std::unique_ptr<ORM> orm);

    // return ret code
    int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

    int InitDatabase() override;

    inline static std::string BuildKeyField(auto&& key, auto&& field) {
      return std::string(key).append("_").append(field);
    }

  protected:
    int update(std::string_view argSV);

    int insert(std::string_view argSV) { return update(argSV); }

    int read(std::string_view argSV);

    int remove(std::string_view argSV);

    int scan(std::string_view argSV);

    int readModifyWrite(std::string_view argSV);

  private:
    std::vector<std::string> fieldNames;
  };
}
