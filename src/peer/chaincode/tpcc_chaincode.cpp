//
// Created by user on 23-8-9.
//

#include "peer/chaincode/tpcc_chaincode.h"
#include "client/core/common/byte_iterator.h"
#include "client/core/common/random_double.h"
#include "client/core/generator/generator.h"
#include "client/tpcc/tpcc_schema.h"
#include "client/tpcc/tpcc_property.h"
#include "common/timer.h"
#include "common/phmap.h"
#include "common/property.h"

namespace peer::chaincode {
    using namespace client::tpcc;

    int chaincode::TPCCChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        return 0;
    }

    int TPCCChaincode::InitDatabase() {
        ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto tpccProperties = client::tpcc::TPCCProperties::NewFromProperty(*property);
        if (!initItem()) {
            return -1;
        }
        auto [beginPartition, endPartition] = TPCCHelper::CalculatePartition(0, 1, tpccProperties->getWarehouseCount());
        if (!initWarehouse(beginPartition, endPartition)) {
            return -1;
        }
        for (int partition = beginPartition; partition < endPartition; partition++) {
            if (!initStock(partition)) {
                return -1;
            }
            if (!initDistrict(TPCCHelper::DISTRICT_COUNT, partition)) {
                return -1;
            }
            if (!initCustomer(TPCCHelper::DISTRICT_COUNT, partition)) {
                return -1;
            }
            if (!initOrder(TPCCHelper::DISTRICT_COUNT, partition)) {
                return -1;
            }
        }
        return 0;
    }

    // partitionID + 1 is the w_id
    bool TPCCChaincode::initStock(int partitionID) {
        if (partitionID < 0) {
            return false; // partition_id start from 0
        }
        // auto tablePrefix = "stock_" + std::to_string(partitionID);
        // init generators
        auto ul = client::core::UniformLongGenerator(10, 100);
        auto dataStrUL = client::core::UniformLongGenerator(26, 50);
        auto xUL = client::core::UniformLongGenerator(1, 10);

        // For each row in the WAREHOUSE table, 100,000 rows in the STOCK table
        for (int i = 0; i < TPCCHelper::ITEMS_COUNT; i++) {
            client::tpcc::schema::stock_t::key_t key{};
            key.s_w_id = partitionID + 1; // W_ID start from 1
            key.s_i_id = i + 1; // S_I_ID also start from 1

            client::tpcc::schema::stock_t value{};
            value.s_quantity = (Integer)ul.nextValue();

            client::utils::RandomString(value.s_dist_01, 24);
            client::utils::RandomString(value.s_dist_02, 24);
            client::utils::RandomString(value.s_dist_03, 24);
            client::utils::RandomString(value.s_dist_04, 24);
            client::utils::RandomString(value.s_dist_05, 24);
            client::utils::RandomString(value.s_dist_06, 24);
            client::utils::RandomString(value.s_dist_07, 24);
            client::utils::RandomString(value.s_dist_08, 24);
            client::utils::RandomString(value.s_dist_09, 24);
            client::utils::RandomString(value.s_dist_10, 24);

            value.s_ytd = 0;
            value.s_order_cnt = 0;
            value.s_remote_cnt = 0;

            /*
             For 10% of the rows, selected at random,
             the string "ORIGINAL" must be held by 8 consecutive characters starting
             at a random position within S_DATA
             */
            auto sDataLen = (int)dataStrUL.nextValue();
            client::utils::RandomString(value.s_data, sDataLen);
            if (xUL.nextValue() == 1) {
                auto posUL = client::core::UniformLongGenerator(0, sDataLen - TPCCHelper::ORIGINAL_STR.size());
                value.s_data.resize(posUL.nextValue());
                value.s_data = value.s_data + TPCCHelper::ORIGINAL_STR;
            }
            if (!insertIntoTable(client::tpcc::TableNamesPrefix::STOCK, key, value)) {
                return false;
            }
        }
        return true;
    }

    bool TPCCChaincode::initItem() {
        // auto tablePrefix = "item_" + std::to_string(partitionID);
        // init generators
        auto imIdUL = client::core::UniformLongGenerator(1, 10000);
        auto nameStrUL = client::core::UniformLongGenerator(14, 24);
        auto priceRD = client::utils::RandomDouble(1, 100);
        auto dataStrUL = client::core::UniformLongGenerator(25, 50);
        auto xUL = client::core::UniformLongGenerator(1, 10);

        // 100,000 rows in the ITEM table
        for (int i = 1; i <= TPCCHelper::ITEMS_COUNT; i++) {
            schema::item_t::key_t key{};
            key.i_id = i;

            schema::item_t value{};
            value.i_im_id = (Integer)imIdUL.nextValue();
            client::utils::RandomString(value.i_name, (int)nameStrUL.nextValue());
            value.i_price = priceRD.nextValue();

            /*
                For 10% of the rows, selected at random,
                the string "ORIGINAL" must be held by 8 consecutive characters
               starting at a random position within I_DATA
            */
            auto sDataLen = (int)dataStrUL.nextValue();
            client::utils::RandomString(value.i_data, sDataLen);
            if (xUL.nextValue() == 1) {
                auto posUL = client::core::UniformLongGenerator(0, sDataLen - TPCCHelper::ORIGINAL_STR.size());
                value.i_data.resize(posUL.nextValue());
                value.i_data = value.i_data + TPCCHelper::ORIGINAL_STR;
            }
            if (!insertIntoTable(client::tpcc::TableNamesPrefix::ITEM, key, value)) {
                return false;
            }
        }
        return true;
    }

    bool TPCCChaincode::initOrder(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "order_" + std::to_string(partitionID);

        // For each row in the WAREHOUSE table, context.n_district rows in the
        // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
        // ORDER table
        std::vector<Integer> c_ids;
        c_ids.reserve(3000);
        for (Integer i = 1; i <= 3000; i++) {
            c_ids.push_back(i);
        }

        auto oOlCntUL = client::core::UniformLongGenerator(5, 14);
        auto oCarrierIdUL = client::core::UniformLongGenerator(1, 10);
        for (int i = 1; i <= nDistrict; i++) {
            std::shuffle(c_ids.begin(), c_ids.end(), *::client::core::GetThreadLocalRandomGenerator());

            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::order_t::key_t key{};
                key.o_w_id = partitionID + 1;
                key.o_d_id = i;
                key.o_id = j;

                client::tpcc::schema::order_t value{};
                value.o_c_id = c_ids[j - 1];
                value.o_entry_d = util::Timer::time_now_ns();

                if (key.o_id < 2101) {
                    value.o_carrier_id = (Integer)oCarrierIdUL.nextValue();
                } else {
                    value.o_carrier_id = 0;
                }

                value.o_ol_cnt = (Numeric)oOlCntUL.nextValue();
                value.o_all_local = true;
                if (!insertIntoTable(client::tpcc::TableNamesPrefix::ORDER, key, value)) {
                    return false;
                }

                // delivery confirm
                client::tpcc::schema::order_wdc_t::key_t wdcKey{};
                wdcKey.o_w_id = key.o_w_id;
                wdcKey.o_d_id = key.o_d_id;
                wdcKey.o_c_id = value.o_c_id;
                wdcKey.o_id = key.o_id;
                if (!insertIntoTable(client::tpcc::TableNamesPrefix::ORDER_WDC, wdcKey, client::tpcc::schema::order_wdc_t{})) {
                    return false;
                }
            }

            for (Integer j = 2100; j <= 3000; j++) {
                client::tpcc::schema::new_order_t::key_t key{};
                key.no_w_id = partitionID + 1;
                key.no_d_id = i;
                key.no_o_id = j;
                if (!insertIntoTable(client::tpcc::TableNamesPrefix::NEW_ORDER, key, client::tpcc::schema::new_order_t{})) {
                    return false;
                }
            }
        }

        return true;
    }

    bool TPCCChaincode::initCustomer(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "customer_" + std::to_string(partitionID);
        auto ul1 = client::core::UniformLongGenerator(8, 16);
        auto ul2 = client::core::UniformLongGenerator(10, 20);
        auto ul3 = client::core::UniformLongGenerator(300, 500);
        auto ul4 = client::core::UniformLongGenerator(12, 24);
        auto ulx = client::core::UniformLongGenerator(1, 10);
        auto rd = client::utils::RandomDouble(0, 0.5);
        for (int i = 1; i <= nDistrict; i++) {
            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::customer_t::key_t key{};
                key.c_w_id = partitionID + 1;
                key.c_d_id = i;
                key.c_id = j;

                client::tpcc::schema::customer_t value{};
                value.c_middle.append('O');
                value.c_middle.append('E');
                client::utils::RandomString(value.c_first, (int)ul1.nextValue());
                client::utils::RandomString(value.c_street_1, (int)ul2.nextValue());
                client::utils::RandomString(value.c_street_2, (int)ul2.nextValue());
                client::utils::RandomString(value.c_city, (int)ul2.nextValue());
                client::utils::RandomString(value.c_state, 2);
                value.c_zip = helper.randomZipCode();
                client::utils::RandomString(value.c_phone, 16);

                value.c_since = util::Timer::time_now_ns();
                value.c_credit_lim = 50000;
                value.c_discount = rd.nextValue();
                value.c_balance = -10;
                value.c_ytd_payment = 1;
                value.c_payment_cnt = 0;
                value.c_delivery_cnt = 0;
                client::utils::RandomString(value.c_data, (int)ul3.nextValue());

                if (j < 1000) {
                    value.c_last = TPCCHelper::GenerateLastName(j - 1);
                } else {
                    value.c_last = TPCCHelper::GenerateLastName(helper.getNonUniformRandomLastNameForLoad());
                }
                // For 10% of the rows, selected at random , C_CREDIT = "BC"
                if (ulx.nextValue() == 1) {
                    value.c_credit.append('B');
                    value.c_credit.append('C');
                } else {
                    value.c_credit.append('G');
                    value.c_credit.append('C');
                }
                if (!insertIntoTable(client::tpcc::TableNamesPrefix::CUSTOMER, key, value)) {
                    return false;
                }
                // init history

                client::tpcc::schema::history_t::key_t historyKey{};
                historyKey.h_c_id = j;
                historyKey.h_c_d_id = i;
                historyKey.h_c_w_id = partitionID + 1;
                historyKey.h_d_id = i;
                historyKey.h_w_id = partitionID + 1;
                historyKey.h_date = util::Timer::time_now_ns();

                client::tpcc::schema::history_t historyValue{};
                historyValue.h_amount = 10;
                client::utils::RandomString(historyValue.h_data, (int)ul4.nextValue());
                if (!insertIntoTable(client::tpcc::TableNamesPrefix::HISTORY, historyKey, historyValue)) {
                    return false;
                }
            }
        }
        return true;
    }

    // nDistrict=10, partitionID = warehouse id - 1
    bool TPCCChaincode::initDistrict(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "district_" + std::to_string(partitionID);
        auto ul1 = client::core::UniformLongGenerator(6, 10);
        auto ul2 = client::core::UniformLongGenerator(10, 20);
        auto rd = client::utils::RandomDouble(0, 0.2);
        // For each row in the WAREHOUSE table, context.n_district rows in the DISTRICT table
        for (int i = 1; i <= nDistrict; i++) {
            client::tpcc::schema::district_t::key_t key{};
            key.d_w_id = partitionID + 1;
            key.d_id = i;

            client::tpcc::schema::district_t value{};
            client::utils::RandomString(value.d_name, (Integer)ul1.nextValue());
            client::utils::RandomString(value.d_street_1, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_street_2, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_city, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_state, 2);
            value.d_zip = helper.randomZipCode();
            value.d_tax = rd.nextValue();
            value.d_ytd = 3000000;
            value.d_next_o_id = 3001;
            if (!insertIntoTable(client::tpcc::TableNamesPrefix::DISTRICT, key, value)) {
                return false;
            }
        }
        return true;
    }

    bool TPCCChaincode::initWarehouse(int fromWhId, int toWhId) {
        // auto tablePrefix = "warehouse_" + std::to_string(partitionID);
        auto ul1 = client::core::UniformLongGenerator(6, 10);
        auto ul2 = client::core::UniformLongGenerator(10, 20);
        auto rd = client::utils::RandomDouble(0.1, 0.2);
        for (auto i = fromWhId; i < toWhId; i++) {
            client::tpcc::schema::warehouse_t::key_t key{};
            key.w_id = i + 1; // partitionID is from 0, W_ID is from 1
            client::tpcc::schema::warehouse_t value{};
            client::utils::RandomString(value.w_name, (int) ul1.nextValue());
            client::utils::RandomString(value.w_street_1, (int) ul2.nextValue());
            client::utils::RandomString(value.w_street_2, (int) ul2.nextValue());
            client::utils::RandomString(value.w_city, (int) ul2.nextValue());
            client::utils::RandomString(value.w_state, 2);
            value.w_zip = helper.randomZipCode();
            value.w_tax = rd.nextValue();
            value.w_ytd = 3000000;
            if (!insertIntoTable(client::tpcc::TableNamesPrefix::WAREHOUSE, key, value)) {
                return false;
            }
        }
        return true;
    }

    template<class Key, class Value>
    bool TPCCChaincode::insertIntoTable(std::string_view tablePrefix, const Key &key, const Value &value) {
        std::string keyRaw(tablePrefix);
        zpp::bits::out outKey(keyRaw);
        outKey.reset(keyRaw.size());
        if(failure(outKey(key))) {
            return false;
        }
        std::string valueRaw;
        valueRaw.reserve(256);
        zpp::bits::out outValue(valueRaw);
        if(failure(outValue(value))) {
            return false;
        }
        orm->put(std::move(keyRaw), std::move(valueRaw));
        return true;
    }
}