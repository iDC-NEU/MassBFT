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
        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::stock_t::key_t& key,
                             const client::tpcc::schema::stock_t& value);

        void insertIntoTable(const client::tpcc::schema::item_t::key_t& key,
                             const client::tpcc::schema::item_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::order_line_t::key_t& key,
                             const client::tpcc::schema::order_line_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::order_wdc_t::key_t& key,
                             const client::tpcc::schema::order_wdc_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::order_t::key_t& key,
                             const client::tpcc::schema::order_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::new_order_t::key_t& key,
                             const client::tpcc::schema::new_order_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::history_t::key_t& key,
                             const client::tpcc::schema::history_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::customer_wdl_t::key_t& key,
                             const client::tpcc::schema::customer_wdl_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::customer_t::key_t& key,
                             const client::tpcc::schema::customer_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::district_t::key_t& key,
                             const client::tpcc::schema::district_t& value);

        bool Get(int partitionID,
                 const client::tpcc::schema::district_t::key_t& key,
                 client::tpcc::schema::district_t& value);

        void insertIntoTable(int partitionID,
                             const client::tpcc::schema::warehouse_t::key_t& key,
                             const client::tpcc::schema::warehouse_t& value);

        std::string buildTablePrefix(const std::string& tableName, int partitionID);

    private:
        client::tpcc::TPCCHelper helper;
    };
}