//
// Created by user on 23-8-9.
//

#include "peer/chaincode/tpcc_chaincode.h"
#include "client/core/common/byte_iterator.h"
#include "client/core/common/random_double.h"
#include "client/core/generator/generator.h"
#include "client/tpcc/tpcc_schema.h"
#include "client/tpcc/tpcc_property.h"
#include "client/tpcc/tpcc_proto.h"
#include "common/timer.h"
#include "common/property.h"

namespace peer::chaincode {
    using namespace client::tpcc;

    int chaincode::TPCCChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        if (funcNameSV == InvokeRequestType::NEW_ORDER) {
            if (executeNewOrder(argSV)) {
                return 0;
            }
            return -1;
        }
        if (funcNameSV == InvokeRequestType::PAYMENT) {
            if (executePayment(argSV)) {
                return 0;
            }
            return -1;
        }
        DLOG(INFO) << "Invalid function call.";
        return -1;
    }

    int TPCCChaincode::InitDatabase() {
        ::client::core::GetThreadLocalRandomGenerator()->seed(0); // use deterministic value
        auto* property = util::Properties::GetProperties();
        auto tpccProperties = TPCCProperties::NewFromProperty(*property);
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
            schema::stock_t::key_t key{};
            key.s_w_id = partitionID + 1; // W_ID start from 1
            key.s_i_id = i + 1; // S_I_ID also start from 1

            schema::stock_t value{};
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
            if (!insertIntoTable(TableNamesPrefix::STOCK, key, value)) {
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
            if (!insertIntoTable(TableNamesPrefix::ITEM, key, value)) {
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
                schema::order_t::key_t key{};
                key.o_w_id = partitionID + 1;
                key.o_d_id = i;
                key.o_id = j;

                schema::order_t value{};
                value.o_c_id = c_ids[j - 1];
                value.o_entry_d = util::Timer::time_now_ns();

                if (key.o_id < 2101) {
                    value.o_carrier_id = (Integer)oCarrierIdUL.nextValue();
                } else {
                    value.o_carrier_id = 0;
                }

                value.o_ol_cnt = (Numeric)oOlCntUL.nextValue();
                value.o_all_local = true;
                if (!insertIntoTable(TableNamesPrefix::ORDER, key, value)) {
                    return false;
                }

                // delivery confirm
                schema::order_wdc_t::key_t wdcKey{};
                wdcKey.o_w_id = key.o_w_id;
                wdcKey.o_d_id = key.o_d_id;
                wdcKey.o_c_id = value.o_c_id;
                wdcKey.o_id = key.o_id;
                if (!insertIntoTable(TableNamesPrefix::ORDER_WDC, wdcKey, schema::order_wdc_t{})) {
                    return false;
                }
            }

            for (Integer j = 2100; j <= 3000; j++) {
                schema::new_order_t::key_t key{};
                key.no_w_id = partitionID + 1;
                key.no_d_id = i;
                key.no_o_id = j;
                if (!insertIntoTable(TableNamesPrefix::NEW_ORDER, key, schema::new_order_t{})) {
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

        // key lastName value: first name and cid
        util::MyNodeHashMap<std::string, std::vector<std::pair<Varchar<16>, int32_t>>> customerMap;
        for (int i = 1; i <= nDistrict; i++) {
            for (int j = 1; j <= 3000; j++) {
                schema::customer_t::key_t key{};
                key.c_w_id = partitionID + 1;
                key.c_d_id = i;
                key.c_id = j;

                schema::customer_t value{};
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
                if (!insertIntoTable(TableNamesPrefix::CUSTOMER, key, value)) {
                    return false;
                }
                // add customer to map
                customerMap[value.c_last.toString()].emplace_back(value.c_first, key.c_id);

                // init history
                schema::history_t::key_t historyKey{};
                historyKey.h_c_id = j;
                historyKey.h_c_d_id = i;
                historyKey.h_c_w_id = partitionID + 1;
                historyKey.h_d_id = i;
                historyKey.h_w_id = partitionID + 1;
                historyKey.h_date = util::Timer::time_now_ns();

                schema::history_t historyValue{};
                historyValue.h_amount = 10;
                client::utils::RandomString(historyValue.h_data, (int)ul4.nextValue());
                if (!insertIntoTable(TableNamesPrefix::HISTORY, historyKey, historyValue)) {
                    return false;
                }
            }

            for (auto& it : customerMap) {
                std::vector<std::pair<Varchar<16>, int32_t>> &list = it.second;
                std::sort(list.begin(), list.end());

                // insert ceiling(n/2) to customer_last_name_idx, n starts from 1
                client::tpcc::schema::customer_wdl_t::key_t cniKey{};
                cniKey.c_w_id = partitionID + 1;
                cniKey.c_d_id = i;
                cniKey.c_last = Varchar<16>(it.first);
                client::tpcc::schema::customer_wdl_t cniValue{};
                cniValue.c_id = list[(list.size() - 1) / 2].second;
                DCHECK(cniValue.c_id > 0) << "C_ID is not valid.";

                if (!insertIntoTable(TableNamesPrefix::CUSTOMER_WDL, cniKey, cniValue)) {
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
            schema::district_t::key_t key{};
            key.d_w_id = partitionID + 1;
            key.d_id = i;

            schema::district_t value{};
            client::utils::RandomString(value.d_name, (Integer)ul1.nextValue());
            client::utils::RandomString(value.d_street_1, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_street_2, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_city, (Integer)ul2.nextValue());
            client::utils::RandomString(value.d_state, 2);
            value.d_zip = helper.randomZipCode();
            value.d_tax = rd.nextValue();
            value.d_ytd = 3000000;
            value.d_next_o_id = 3001;
            if (!insertIntoTable(TableNamesPrefix::DISTRICT, key, value)) {
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
            schema::warehouse_t::key_t key{};
            key.w_id = i + 1; // partitionID is from 0, W_ID is from 1
            schema::warehouse_t value{};
            client::utils::RandomString(value.w_name, (int) ul1.nextValue());
            client::utils::RandomString(value.w_street_1, (int) ul2.nextValue());
            client::utils::RandomString(value.w_street_2, (int) ul2.nextValue());
            client::utils::RandomString(value.w_city, (int) ul2.nextValue());
            client::utils::RandomString(value.w_state, 2);
            value.w_zip = helper.randomZipCode();
            value.w_tax = rd.nextValue();
            value.w_ytd = 3000000;
            if (!insertIntoTable(TableNamesPrefix::WAREHOUSE, key, value)) {
                return false;
            }
        }
        return true;
    }

    bool TPCCChaincode::executeNewOrder(std::string_view argSV) {
        ::client::tpcc::proto::NewOrder newOrder(0);
        auto in = zpp::bits::in(argSV);
        if(failure(in(newOrder))) {
            return false;
        }

        Numeric w_tax;
        {
            // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
            // the warehouse tax rate, is retrieved.
            schema::warehouse_t::key_t wKey { .w_id = newOrder.warehouseId };
            schema::warehouse_t wValue;
            if (!getValue(TableNamesPrefix::WAREHOUSE, wKey, wValue)) {
                return false;
            }
            w_tax = wValue.w_tax;
        }

        Numeric d_tax;
        Integer o_id;
        {
            // The row in the DISTRICT table with matching D_W_ID and D_ID is selected,
            // D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
            // available order number for the district, is retrieved and incremented by one.
            schema::district_t::key_t dKey {
                    .d_w_id = newOrder.warehouseId,
                    .d_id = newOrder.districtId,
            };
            schema::district_t dValue;
            if (!getValue(TableNamesPrefix::DISTRICT, dKey, dValue)) {
                return false;
            }
            d_tax = dValue.d_tax;
            o_id = dValue.d_next_o_id++;
            if (!insertIntoTable(TableNamesPrefix::DISTRICT, dKey, dValue)) {
                return false;
            }
        }

        Numeric c_discount;
        {
            // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
            // selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
            // customer's last name, and C_CREDIT, the customer's credit status, are retrieved.
            schema::customer_t::key_t cKey {
                    .c_w_id = newOrder.warehouseId,
                    .c_d_id = newOrder.districtId,
                    .c_id = newOrder.customerId,
            };
            schema::customer_t cValue;
            if (!getValue(TableNamesPrefix::CUSTOMER, cKey, cValue)) {
                return false;
            }
            c_discount = cValue.c_discount;
        }

        Numeric all_local = 1;
        for (const auto& sw : newOrder.supplierWarehouse) {
            if (sw != newOrder.warehouseId) {
                all_local = 0;
            }
        }
        // order insert
        {
            schema::order_t::key_t key {
                    .o_w_id = newOrder.warehouseId,
                    .o_d_id = newOrder.districtId,
                    .o_id = o_id,
            };
            schema::order_t value {
                    .o_c_id = newOrder.customerId,
                    .o_entry_d = newOrder.timestamp,
                    .o_carrier_id = 0,
                    .o_ol_cnt = (Numeric)newOrder.orderLineCount,
                    .o_all_local = all_local,
            };
            if (!insertIntoTable(TableNamesPrefix::ORDER, key, value)) {
                return false;
            }
        }
        // order wdc insert
        {
            schema::order_wdc_t::key_t key {
                    .o_w_id = newOrder.warehouseId,
                    .o_d_id = newOrder.districtId,
                    .o_c_id = newOrder.customerId,
                    .o_id = o_id,
            };
            if (!insertIntoTable(TableNamesPrefix::ORDER_WDC, key, schema::order_wdc_t{})) {
                return false;
            }
        }
        // new order insert
        {
            schema::new_order_t::key_t key {
                .no_w_id = newOrder.warehouseId,
                .no_d_id = newOrder.districtId,
                .no_o_id = o_id,
            };
            if (!insertIntoTable(TableNamesPrefix::NEW_ORDER, key, schema::new_order_t{})) {
                return false;
            }
        }

        for (auto i = 0; i < newOrder.orderLineCount; i++) {
            const auto& ol_i_id = newOrder.itemIds[i];
            const auto& ol_quantity = newOrder.quantities[i];
            const auto& ol_supply_w_id = newOrder.supplierWarehouse[i];
            {
                // The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
                // S_W_ID (equals OL_SUPPLY_W_ID) is selected.
                schema::stock_t::key_t stockKey {
                        .s_w_id = ol_supply_w_id,
                        .s_i_id = ol_i_id,
                };
                schema::stock_t stockValue;
                if (!getValue(TableNamesPrefix::STOCK, stockKey, stockValue)) {
                    return false;
                }
                if (stockValue.s_quantity >= ol_quantity + 10) {
                    stockValue.s_quantity -= ol_quantity;
                } else {
                    stockValue.s_quantity += 91 - ol_quantity;
                }
                if (ol_supply_w_id != newOrder.warehouseId) {
                    stockValue.s_remote_cnt += 1;
                }
                stockValue.s_order_cnt += 1;
                stockValue.s_ytd += ol_quantity;
                if (!insertIntoTable(TableNamesPrefix::STOCK, stockKey, stockValue)) {
                    return false;
                }
            }
            const auto& line_number = newOrder.orderLineNumbers[i];
            Numeric i_price;
            {
                // find the item
                schema::item_t::key_t itemKey{.i_id = ol_i_id};
                schema::item_t itemValue;
                if (!getValue(TableNamesPrefix::ITEM, itemKey, itemValue)) {
                    DLOG(INFO) << "Item search miss, rollback.";
                    return false;
                }
                i_price = itemValue.i_price;
            }
            Varchar<24> s_dist;
            {
                schema::stock_t::key_t stockKey {
                        .s_w_id = newOrder.warehouseId,
                        .s_i_id = ol_i_id,
                };
                schema::stock_t stockValue;
                if (!getValue(TableNamesPrefix::STOCK, stockKey, stockValue)) {
                    return false;
                }
                switch (newOrder.districtId) {
                    case 1:
                        s_dist = stockValue.s_dist_01;
                        break;
                    case 2:
                        s_dist = stockValue.s_dist_02;
                        break;
                    case 3:
                        s_dist = stockValue.s_dist_03;
                        break;
                    case 4:
                        s_dist = stockValue.s_dist_04;
                        break;
                    case 5:
                        s_dist = stockValue.s_dist_05;
                        break;
                    case 6:
                        s_dist = stockValue.s_dist_06;
                        break;
                    case 7:
                        s_dist = stockValue.s_dist_07;
                        break;
                    case 8:
                        s_dist = stockValue.s_dist_08;
                        break;
                    case 9:
                        s_dist = stockValue.s_dist_09;
                        break;
                    case 10:
                        s_dist = stockValue.s_dist_10;
                        break;
                    default:
                        CHECK(false) << "District id mismatch!";
                }
            }
            Numeric ol_amount = ol_quantity * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
            // Timestamp ol_delivery_d = util::Timer::time_now_ns();
            Timestamp ol_delivery_d = 0;
            {
                schema::order_line_t::key_t key {
                        .ol_w_id = newOrder.warehouseId,
                        .ol_d_id = newOrder.districtId,
                        .ol_o_id = newOrder.districtId,
                        .ol_number = line_number,
                };
                schema::order_line_t value {
                        .ol_i_id = ol_i_id,
                        .ol_supply_w_id = ol_supply_w_id,
                        .ol_delivery_d = ol_delivery_d,
                        .ol_quantity = static_cast<Numeric>(ol_quantity),
                        .ol_amount = ol_amount,
                        .ol_dist_info = s_dist,
                };
                if (!insertIntoTable(TableNamesPrefix::ORDER_LINE, key, value)) {
                    return false;
                }
            }
        }
        return true;
    }

    bool TPCCChaincode::executePayment(std::string_view argSV) {
        ::client::tpcc::proto::Payment payment{};
        auto in = zpp::bits::in(argSV);
        if(failure(in(payment))) {
            return false;
        }

        schema::warehouse_t wValue;
        {
            schema::warehouse_t::key_t wKey{.w_id = payment.warehouseId};
            if (!getValue(TableNamesPrefix::WAREHOUSE, wKey, wValue)) {
                return false;
            }
            // cache the old value
            Numeric w_ytd = wValue.w_ytd;
            wValue.w_ytd += payment.homeOrderTotalAmount;
            if (!insertIntoTable(TableNamesPrefix::WAREHOUSE, wKey, wValue)) {
                return false;
            }
            // restore the original value
            wValue.w_ytd = w_ytd;
        }
        schema::district_t dValue;
        {
            schema::district_t::key_t dKey {
                    .d_w_id = payment.warehouseId,
                    .d_id = payment.districtId,
            };
            if (!getValue(TableNamesPrefix::DISTRICT, dKey, dValue)) {
                return false;
            }
            // cache the old value
            Numeric d_ytd = dValue.d_ytd;
            dValue.d_ytd += payment.homeOrderTotalAmount;
            if (!insertIntoTable(TableNamesPrefix::DISTRICT, dKey, dValue)) {
                return false;
            }
            // restore the original value
            dValue.d_ytd = d_ytd;
        }

        Integer c_id;
        if (payment.isPaymentById) {
            c_id = payment.customerId;
        } else {
            schema::customer_wdl_t::key_t wdlKey {
                    .c_w_id = payment.customerWarehouseId,
                    .c_d_id = payment.customerDistrictId,
                    .c_last = payment.customerLastName,
            };
            schema::customer_wdl_t wdlValue {};
            if (!getValue(TableNamesPrefix::CUSTOMER_WDL, wdlKey, wdlValue)) {
                DLOG(INFO) << "Can not find customer id";
                return false;
            }
            c_id = wdlValue.c_id;
            DCHECK(c_id > 0) << "Invalid C_ID read from index";
        }
        {
            schema::customer_t::key_t cKey {
                    .c_w_id = payment.customerWarehouseId,
                    .c_d_id = payment.customerDistrictId,
                    .c_id = c_id,
            };
            schema::customer_t cValue;
            if (!getValue(TableNamesPrefix::CUSTOMER, cKey, cValue)) {
                return false;
            }

            // update c_data
            if (cValue.c_credit[0] == 'B' && cValue.c_credit[1] == 'C') {
                std::string valueRaw;
                valueRaw.reserve(1024);
                zpp::bits::out outValue(valueRaw);
                if(failure(outValue(c_id, payment, wValue.w_name, dValue.d_name, cValue.c_data))) {
                    return false;
                }
                valueRaw.resize(500);
                cValue.c_data = Varchar<500>(valueRaw);
            } else {
                DCHECK(cValue.c_credit[0] == 'G' && cValue.c_credit[1] == 'C');
            }

            // update values
            cValue.c_balance -= payment.homeOrderTotalAmount;
            cValue.c_ytd_payment += payment.homeOrderTotalAmount;
            cValue.c_payment_cnt += 1;

            if (!insertIntoTable(TableNamesPrefix::CUSTOMER, cKey, cValue)) {
                return false;
            }
        }
        {
            std::string h_new_data = wValue.w_name.toString() + "    " + dValue.d_name.toString();
            if (h_new_data.size() > 24) {
                h_new_data.resize(24);
            }
            schema::history_t::key_t hKey {
                    .h_c_id = c_id,
                    .h_c_d_id = payment.customerDistrictId,
                    .h_c_w_id = payment.customerWarehouseId,
                    .h_d_id = payment.districtId,
                    .h_w_id = payment.warehouseId,
                    .h_date = payment.timestamp,
            };
            schema::history_t hValue {
                .h_amount = payment.homeOrderTotalAmount,
                .h_data = Varchar<24>(h_new_data),
            };

            if (!insertIntoTable(TableNamesPrefix::HISTORY, hKey, hValue)) {
                return false;
            }
        }
        return true;
    }

    template<class Key, class Value>
    bool TPCCChaincode::getValue(std::string_view tablePrefix, const Key &key, Value &value) {
        std::string keyRaw(tablePrefix);
        zpp::bits::out outKey(keyRaw);
        outKey.reset(keyRaw.size());
        if(failure(outKey(key))) {
            return false;
        }
        std::string_view valueSV;
        if (!orm->get(std::move(keyRaw), &valueSV)) {
            return false;
        }
        zpp::bits::in inValue(valueSV);
        if(failure(inValue(value))) {
            return false;
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