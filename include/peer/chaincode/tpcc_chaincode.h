//
// Created by user on 23-7-12.
//

#pragma once

#include "peer/chaincode/chaincode.h"
#include "client/tpcc/tpcc_schema.h"
#include "client/tpcc/tpcc_helper.h"

namespace peer::chaincode {
    class TPCCChaincode : public Chaincode {
    public:
        explicit TPCCChaincode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

    protected:
        bool initStock(int partitionID);

        // item is read-only and will be replicated anyway
        bool initItem();

        bool initOrder(int nDistrict, int partitionID);

        bool initCustomer(int nDistrict, int partitionID);

        bool initDistrict(int nDistrict, int partitionID);

        // [fromWhId, toWhId)
        bool initWarehouse(int fromWhId, int toWhId);

    protected:
        bool executeNewOrder(std::string_view argSV);

    protected:
        template<class Key, class Value>
        inline bool insertIntoTable(std::string_view tablePrefix, const Key& key, const Value& value);

        template<class Key, class Value>
        inline bool getValue(std::string_view tablePrefix, const Key& key, Value& value);


    private:
        client::tpcc::TPCCHelper helper;
    };
}