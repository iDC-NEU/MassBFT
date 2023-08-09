//
// Created by user on 23-7-12.
//

#pragma once

#include "peer/chaincode/chaincode.h"
#include "client/tpcc/tpcc_schema.h"

namespace peer::chaincode {
    class TPCCChaincode : public Chaincode {
    public:
        explicit TPCCChaincode(std::unique_ptr<ORM> orm_) : Chaincode(std::move(orm_)) { }

        int InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) override;

        int InitDatabase() override;

    protected:
        bool initStock(int partitionID);

        bool initItem(int partitionID);

        bool initOrderLine(int nDistrict, int partitionID);

        bool initOrder(int nDistrict, int partitionID);

        bool initNewOrder(int nDistrict, int partitionID);

        bool initHistory(int nDistrict, int partitionID);

        bool initCustomerNameIdx(int nDistrict, int partitionID);

        bool initCustomer(int nDistrict, int partitionID);

        bool initDistrict(int nDistrict, int partitionID);

        bool initWarehouse(int partitionID);

    protected:
        bool executeNewOrder(std::string_view argSV);

    protected:
        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::StockKey& key,
                             const client::tpcc::schema::StockValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::ItemKey& key,
                             const client::tpcc::schema::ItemValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::OrderLineKey& key,
                             const client::tpcc::schema::OrderLineValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::OrderKey& key,
                             const client::tpcc::schema::OrderValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::NewOrderKey& key,
                             const client::tpcc::schema::NewOrderValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::HistoryKey& key,
                             const client::tpcc::schema::HistoryValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::CustomerNameIdxKey& key,
                             const client::tpcc::schema::CustomerNameIdxValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::CustomerKey& key,
                             const client::tpcc::schema::CustomerValue& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::DistrictKey& key,
                             const client::tpcc::schema::DistrictValues& value);

        bool Get(int partitionID,
                 const client::tpcc::schema::DistrictKey& key,
                 client::tpcc::schema::DistrictValues& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::WarehouseKey& key,
                             const client::tpcc::schema::WarehouseValue& value);

        std::string buildTablePrefix(const std::string& tableName, int partitionID);

        static void GenerateLastName(auto& buf, int n) {
            const auto &s1 = lastNames[n / 100];
            const auto &s2 = lastNames[n / 10 % 10];
            const auto &s3 = lastNames[n % 10];
            auto name = s1 + s2 + s3;
            if (name.size() > buf.size()) {
                name.resize(buf.size());
            }
            std::copy(name.begin(), name.end(), buf.begin());
        }

    private:
        inline static const std::string ORIGINAL_STR = "ORIGINAL";

        inline static const std::vector<std::string> lastNames = {
                "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
                "ESE", "ANTI",  "CALLY", "ATION", "EING"
        };
    };
}