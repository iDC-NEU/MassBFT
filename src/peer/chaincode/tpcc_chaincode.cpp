//
// Created by user on 23-8-9.
//

#include "peer/chaincode/tpcc_chaincode.h"
#include "client/core/common/byte_iterator.h"
#include "client/core/common/random_double.h"
#include "client/core/generator/non_uniform_generator.h"
#include "client/tpcc/tpcc_schema.h"
#include "common/timer.h"
#include "common/phmap.h"

namespace peer::chaincode {
    int chaincode::TPCCChaincode::InvokeChaincode(std::string_view funcNameSV, std::string_view argSV) {
        return 0;
    }

    int TPCCChaincode::InitDatabase() {

    }

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
        for (int i = 1; i <= 100000; i++) {
            client::tpcc::schema::StockKey key{};
            key.S_W_ID = partitionID + 1; // W_ID start from 1
            key.S_I_ID = i; // S_I_ID also start from 1

            client::tpcc::schema::StockValue value{};
            value.S_QUANTITY = ul.nextValue();

            for (auto& it: value.S_DIST) {
                client::utils::RandomString(it, 25);
            }
            value.S_YTD = 0;
            value.S_ORDER_CNT = 0;
            value.S_REMOTE_CNT = 0;

            /*
             For 10% of the rows, selected at random,
             the string "ORIGINAL" must be held by 8 consecutive characters starting
             at a random position within S_DATA
             */
            auto sDataLen = (int)dataStrUL.nextValue();
            client::utils::RandomString(value.S_DATA, sDataLen);
            if (xUL.nextValue() == 1) {
                auto posUL = client::core::UniformLongGenerator(0, sDataLen - ORIGINAL_STR.length());
                std::copy(ORIGINAL_STR.begin(), ORIGINAL_STR.end(), value.S_DATA.begin() + posUL.nextValue());
            }
            insertIntoTable(partitionID, key, value);
        }
        return true;
    }

    bool TPCCChaincode::initItem(int partitionID) {
        if (partitionID < 0) {
            return false; // partition_id start from 0
        }
        // auto tablePrefix = "item_" + std::to_string(partitionID);
        // init generators
        auto imIdUL = client::core::UniformLongGenerator(1, 10000);
        auto nameStrUL = client::core::UniformLongGenerator(14, 24);
        auto priceRD = client::utils::RandomDouble(1, 100);
        auto dataStrUL = client::core::UniformLongGenerator(26, 50);
        auto xUL = client::core::UniformLongGenerator(1, 10);

        // 100,000 rows in the ITEM table
        for (int i = 1; i <= 100000; i++) {
            client::tpcc::schema::ItemKey key{};
            key.I_ID = i;

            client::tpcc::schema::ItemValue value{};
            value.I_IM_ID = imIdUL.nextValue();
            client::utils::RandomString(value.I_NAME, (int)nameStrUL.nextValue());
            value.I_PRICE = priceRD.nextValue();

            /*
                For 10% of the rows, selected at random,
                the string "ORIGINAL" must be held by 8 consecutive characters
               starting at a random position within I_DATA
            */
            auto sDataLen = (int)dataStrUL.nextValue();
            client::utils::RandomString(value.I_DATA, sDataLen);
            if (xUL.nextValue() == 1) {
                auto posUL = client::core::UniformLongGenerator(0, sDataLen - ORIGINAL_STR.length());
                std::copy(ORIGINAL_STR.begin(), ORIGINAL_STR.end(), value.I_DATA.begin() + posUL.nextValue());
            }
            insertIntoTable(partitionID, key, value);
        }
        return true;
    }

    bool TPCCChaincode::initOrderLine(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "order_line_" + std::to_string(partitionID);
        auto orderTable = buildTablePrefix("order_table", partitionID);
        auto olIIdUL = client::core::UniformLongGenerator(1, 100000);
        auto olAmountRD = client::utils::RandomDouble(1, 9999);

        // For each row in the WAREHOUSE table, context.n_district rows in the DISTRICT table
        for (int i = 1; i <= nDistrict; i++) {
            client::tpcc::schema::OrderKey orderKey{};
            orderKey.O_W_ID = partitionID + 1;
            orderKey.O_D_ID = i;

            // For each row in the DISTRICT table, 3,000 rows in the ORDER table
            for (int j = 1; j <= 3000; j++) {
                orderKey.O_ID = j;
                // no concurrent write, it is ok to read without validation on MetaDataType
                client::tpcc::schema::OrderValue orderValue{};
                std::string_view rawValue;
                auto ret = orm->get(orderKey.buildTableKey(orderTable), &rawValue);
                if (!ret) {
                    LOG(ERROR) << "order value not found!";
                    return false;
                }
                if (!orderValue.deserializeFromString(rawValue)) {
                    return false;
                }
                // For each row in the ORDER table, O_OL_CNT rows in the ORDER_LINE table
                for (int k = 1; k <= orderValue.O_OL_CNT; k++) {
                    client::tpcc::schema::OrderLineKey key{};
                    key.OL_W_ID = partitionID + 1;
                    key.OL_D_ID = i;
                    key.OL_O_ID = j;
                    key.OL_NUMBER = k;

                    client::tpcc::schema::OrderLineValue value{};
                    value.OL_I_ID = olIIdUL.nextValue();
                    value.OL_SUPPLY_W_ID = partitionID + 1;
                    value.OL_QUANTITY = 5;
                    client::utils::RandomString(value.OL_DIST_INFO, 25);

                    if (key.OL_O_ID < 2101) {
                        value.OL_DELIVERY_D = orderValue.O_ENTRY_D;
                        value.OL_AMOUNT = 0;
                    } else {
                        value.OL_DELIVERY_D = 0;
                        value.OL_AMOUNT = olAmountRD.nextValue();
                    }
                    insertIntoTable(partitionID, key, value);
                }
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
        std::vector<int> perm;
        perm.reserve(3000);
        for (int i = 1; i <= 3000; i++) {
            perm.push_back(i);
        }

        auto oOlCntUL = client::core::UniformLongGenerator(5, 15);
        auto oCarrierIdUL = client::core::UniformLongGenerator(1, 10);
        for (int i = 1; i <= nDistrict; i++) {
            std::shuffle(perm.begin(), perm.end(), *::client::core::GetThreadLocalRandomGenerator());

            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::OrderKey key{};
                key.O_W_ID = partitionID + 1;
                key.O_D_ID = i;
                key.O_ID = j;

                client::tpcc::schema::OrderValue value{};
                value.O_C_ID = perm[j - 1];
                value.O_ENTRY_D = util::Timer::time_now_ns();
                value.O_OL_CNT = oOlCntUL.nextValue();
                value.O_ALL_LOCAL = true;

                if (key.O_ID < 2101) {
                    value.O_CARRIER_ID = oCarrierIdUL.nextValue();
                } else {
                    value.O_CARRIER_ID = 0;
                }

                insertIntoTable(partitionID, key, value);
            }
        }
        return true;
    }

    bool TPCCChaincode::initNewOrder(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "new_order_" + std::to_string(partitionID);

        // For each row in the WAREHOUSE table, context.n_district rows in the DISTRICT table
        for (int i = 1; i <= nDistrict; i++) {
            // For each row in the DISTRICT table, 3,000 rows in the ORDER table
            // For each row in the ORDER table from 2101 to 3000, 1 row in the NEW_ORDER table
            for (int j = 2101; j <= 3000; j++) {
                client::tpcc::schema::NewOrderKey key{};
                key.NO_W_ID = partitionID + 1;
                key.NO_D_ID = i;
                key.NO_O_ID = j;
                client::tpcc::schema::NewOrderValue value{};

                insertIntoTable(partitionID, key, value);
            }
        }
        return true;
    }

    bool TPCCChaincode::initHistory(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "history_" + std::to_string(partitionID);

        // For each row in the WAREHOUSE table, context.n_district rows in the
        // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
        // CUSTOMER table For each row in the CUSTOMER table, 1 row in the HISTORY
        // table
        for (int i = 1; i <= nDistrict; i++) {
            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::HistoryKey key{};
                key.H_W_ID = partitionID + 1;
                key.H_D_ID = i;
                key.H_C_W_ID = partitionID + 1;
                key.H_C_D_ID = i;
                key.H_C_ID = j;
                key.H_DATE = util::Timer::time_now_ns();

                client::tpcc::schema::HistoryValue value{};
                value.H_AMOUNT = 10;
                client::utils::RandomString(value.H_DATA, 25);

                insertIntoTable(partitionID, key, value);
            }
        }
        return true;
    }

    bool TPCCChaincode::initCustomerNameIdx(int nDistrict, int partitionID) {
        if (partitionID < 0 || nDistrict <= 0) {
            return false; // partition_id start from 0, nDistrict start from 1
        }
        // auto tablePrefix = "customer_name_" + std::to_string(partitionID);

        // For each row in the WAREHOUSE table, context.n_district rows in the
        // DISTRICT table For each row in the DISTRICT table, 3,000 rows in the
        // CUSTOMER table
        auto customerNameTable = buildTablePrefix("customer_name_table", partitionID);

        // key lastName value: first name and cid
        util::MyNodeHashMap<std::string, std::vector<std::pair<std::array<char, 16>, int32_t>>> map;

        for (int i = 1; i <= nDistrict; i++) {
            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::CustomerKey customerKey{};
                customerKey.C_W_ID = partitionID + 1;
                customerKey.C_D_ID = i;
                customerKey.C_ID = j;

                // no concurrent write, it is ok to read without validation on MetaDataType
                client::tpcc::schema::CustomerValue customerValue{};
                std::string_view rawValue;
                auto ret = orm->get(customerKey.buildTableKey(customerNameTable), &rawValue);
                if (!ret) {
                    LOG(ERROR) << "customer name value not found!";
                    return false;
                }
                if (!customerValue.deserializeFromString(rawValue)) {
                    return false;
                }
                std::string_view sv(customerValue.C_LAST.data(), customerValue.C_LAST.size());
                map.at(sv).emplace_back(customerValue.C_FIRST, customerKey.C_ID);
            }

            for (auto& it : map) {
                std::vector<std::pair<std::array<char, 16>, int32_t>> &v = it.second;
                std::sort(v.begin(), v.end());

                // insert ceiling(n/2) to customer_last_name_idx, n starts from 1
                client::tpcc::schema::CustomerNameIdxKey cniKey{};
                cniKey.C_W_ID = partitionID + 1;
                cniKey.C_D_ID = i;
                if (it.first.size() > cniKey.C_LAST.size()) {
                    return false;
                }
                std::copy(it.first.begin(), it.first.end(), cniKey.C_LAST.begin());
                client::tpcc::schema::CustomerNameIdxValue cniValue{};
                cniValue.C_ID = v[(v.size() - 1) / 2].second;
                CHECK(cniValue.C_ID > 0) << "C_ID is not valid.";

                insertIntoTable(partitionID, cniKey, cniValue);
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
        auto ulx = client::core::UniformLongGenerator(1, 10);
        auto rd = client::utils::RandomDouble(0, 0.5);
        auto nul = client::core::NonUniformGenerator(255, 0, 999);
        for (int i = 1; i <= nDistrict; i++) {
            for (int j = 1; j <= 3000; j++) {
                client::tpcc::schema::CustomerKey key{};
                key.C_W_ID = partitionID + 1;
                key.C_D_ID = i;
                key.C_ID = j;

                client::tpcc::schema::CustomerValue value{};
                value.C_MIDDLE = {'O', 'E' };
                client::utils::RandomString(value.C_FIRST, (int)ul1.nextValue());
                client::utils::RandomString(value.C_STREET_1, (int)ul2.nextValue());
                client::utils::RandomString(value.C_STREET_2, (int)ul2.nextValue());
                client::utils::RandomString(value.C_CITY, (int)ul2.nextValue());
                client::utils::RandomString(value.C_STATE, 2);
                client::utils::RandomString(value.C_ZIP, 9);
                client::utils::RandomString(value.C_PHONE, 16);

                value.C_SINCE = util::Timer::time_now_ns();
                value.C_CREDIT_LIM = 50000;
                value.C_DISCOUNT = rd.nextValue();
                value.C_BALANCE = -10;
                value.C_YTD_PAYMENT = 10;
                value.C_PAYMENT_CNT = 1;
                value.C_DELIVERY_CNT = 1;
                value.C_DATA = client::utils::RandomString((int)ul3.nextValue());

                int idx = j - 1;
                if (j > 1000) {
                    idx = (int)nul.nextValue();
                }
                GenerateLastName(value.C_LAST, idx);
                // For 10% of the rows, selected at random , C_CREDIT = "BC"
                if (ulx.nextValue() == 1) {
                    value.C_CREDIT = {'B', 'C' };
                } else {
                    value.C_CREDIT = {'G', 'C' };
                }
                insertIntoTable(partitionID, key, value);
            }
        }
        return true;
    }

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
            client::tpcc::schema::DistrictKey key{};
            key.D_W_ID = partitionID + 1;
            key.D_ID = i;

            client::tpcc::schema::DistrictValue value{};
            client::utils::RandomString(value.D_NAME, (int)ul1.nextValue());
            client::utils::RandomString(value.D_STREET_1, (int)ul2.nextValue());
            client::utils::RandomString(value.D_STREET_2, (int)ul2.nextValue());
            client::utils::RandomString(value.D_CITY, (int)ul2.nextValue());
            client::utils::RandomString(value.D_STATE, 2);
            client::utils::RandomString(value.D_ZIP, 9);

            value.D_TAX = rd.nextValue();
            value.D_YTD = 30000;
            value.D_NEXT_O_ID = 3001;
            insertIntoTable(partitionID, key, value);
        }
    }

    bool TPCCChaincode::initWarehouse(int partitionID) {
        if (partitionID < 0) {
            return false; // partition_id start from 0
        }
        // auto tablePrefix = "warehouse_" + std::to_string(partitionID);

        client::tpcc::schema::WarehouseKey key{};
        key.W_ID = partitionID + 1; // partitionID is from 0, W_ID is from 1

        auto ul1 = client::core::UniformLongGenerator(6, 10);
        auto ul2 = client::core::UniformLongGenerator(10, 20);
        auto rd = client::utils::RandomDouble(0, 0.2);
        client::tpcc::schema::WarehouseValue value{};
        client::utils::RandomString(value.W_NAME, (int)ul1.nextValue());
        client::utils::RandomString(value.W_STREET_1, (int)ul2.nextValue());
        client::utils::RandomString(value.W_STREET_2, (int)ul2.nextValue());
        client::utils::RandomString(value.W_CITY, (int)ul2.nextValue());
        client::utils::RandomString(value.W_STATE, 2);
        client::utils::RandomString(value.W_ZIP, 9);
        value.W_TAX = rd.nextValue();
        value.W_YTD = 30000;
        insertIntoTable(partitionID, key, value);
    }

    bool TPCCChaincode::executeNewOrder(std::string_view argSV) {
        // district
        client::tpcc::schema::DistrictKey districtKey{};
        auto in = zpp::bits::in(argSV);
        if(failure(in(districtKey))) {
            return false;
        }
        client::tpcc::schema::DistrictValue row{};
        auto partitionID = districtKey.D_W_ID - 1;
        DCHECK(partitionID >= 0);
        if (!Get(partitionID, districtKey, row)) {
            return false;
        }

        auto row = tbl_district_vec[]->search(&district_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);
        district::value &district_value =
                *static_cast<district::value *>(std::get<1>(row));
        dec >> district_value.D_NEXT_O_ID;

        // stock
        auto stockTableID = stock::tableID;
        while (dec.size() > 0) {
            stock::key stock_key;
            dec >> stock_key.S_W_ID >> stock_key.S_I_ID;

            auto row = tbl_stock_vec[stock_key.S_W_ID - 1]->search(&stock_key);
            MetaDataType &tid = *std::get<0>(row);
            tid.store(operation.tid);
            stock::value &stock_value =
                    *static_cast<stock::value *>(std::get<1>(row));

            dec >> stock_value.S_QUANTITY >> stock_value.S_YTD >>
                stock_value.S_ORDER_CNT >> stock_value.S_REMOTE_CNT;
        }
    }

}