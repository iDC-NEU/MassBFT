//
// Created by user on 23-8-9.
//

#pragma once

#include "zpp_bits.h"
#include <array>

namespace client::tpcc::schema {
    struct StockKey {
        // Stock Warehouse Identifier, The warehouse identifier to which the item belongs
        uint32_t S_W_ID;
        // Stock Item Identifier, Unique identifier for the item in stock
        uint32_t S_I_ID;
    };

    struct StockValue {
        // S_QUANTITY stores the inventory quantity of each item in the warehouse
        uint16_t S_QUANTITY;
        // S_DIST_01 to S_DIST_10 represent the distribution information of ten different districts of the warehouse
        std::array<char, 25> S_DIST[10];
        // S_YTD means "Stock Year-To-Date", which is the cumulative annual sales of inventory items
        double S_YTD;
        // S_ORDER_CNT is the abbreviation of "Stock Order Count", which means the order count of stock items
        uint32_t S_ORDER_CNT;
        // S_REMOTE_CNT indicates the "Remote Count" of the inventory item,
        // that is, the quantity of the inventory item distributed in other warehouses.
        // In a distributed database environment, a warehouse may be distributed
        // in multiple geographical locations or servers,
        // and each warehouse may maintain a part of the commodity inventory.
        uint32_t S_REMOTE_CNT;
        // S_DATA refers to a data field in the stock table (STOCK table),
        // which stores additional information about stock items
        std::array<char, 50> S_DATA;
    };

    struct ItemKey {
        // Item Identification Number
        uint32_t I_ID;
    };

    struct ItemValue {
        // Item Image Identifier, indicating the identification number of the product image.
        // Each product can be associated with an image,
        // and this identification number is used to identify the relevant information of the product image.
        uint32_t I_IM_ID;
        // Item name
        std::array<char, 25> I_NAME;
        // item price
        double I_PRICE;
        std::array<char, 50> I_DATA;
    };

    struct OrderKey {
        uint32_t O_W_ID;
        uint32_t O_D_ID;
        uint32_t O_ID;

        std::string buildTableKey(const std::string& prefix) {
            std::stringstream ss;
            ss << prefix << "_" << O_W_ID << "_" << O_D_ID << "_" << O_ID;
            return ss.str();
        }
    };

    struct OrderValue {
        // "Order-Line Count". It refers to the number of order lines in an order,
        // and is used to indicate how many order line items are included in an order.
        uint8_t O_OL_CNT;
        uint64_t O_ENTRY_D;
        // Order Customer Identification Number
        uint32_t O_C_ID;
        bool O_ALL_LOCAL;
        uint32_t O_CARRIER_ID;

        bool deserializeFromString(std::string_view raw) {
            auto in = zpp::bits::in(raw);
            if(failure(in(*this))) {
                return false;
            }
            return true;
        }
    };

    struct OrderLineKey {
        // Order Line Warehouse ID, which indicates the warehouse identification number to which the order line belongs
        uint32_t OL_W_ID;
        // Order Line District ID, indicating the identification number of the district to which the order line belongs
        uint32_t OL_D_ID;
        // Order Line Order ID, indicating the order identification number of the order line item.
        uint32_t OL_O_ID;
        // Order Line Number, indicating the order line number
        uint8_t OL_NUMBER;
    };

    struct OrderLineValue {
        uint32_t OL_I_ID;
        uint32_t OL_SUPPLY_W_ID;
        uint8_t OL_QUANTITY;
        std::array<char, 25> OL_DIST_INFO;
        uint64_t OL_DELIVERY_D;
        double OL_AMOUNT;
    };

    struct NewOrderKey {
        uint32_t NO_W_ID;
        uint32_t NO_D_ID;
        uint32_t NO_O_ID;
    };

    struct NewOrderValue {
        // New Order DUMMY means that in the New-Order test transaction,
        // the actual order generation operation is not performed,
        // but only the execution of the simulated transaction,
        // thereby reducing the load and overhead of the database.
        uint32_t NO_DUMMY;
    };

    struct HistoryKey {
        uint32_t H_W_ID;
        uint32_t H_D_ID;
        uint32_t H_C_W_ID;
        uint32_t H_C_D_ID;
        uint32_t H_C_ID;
        uint64_t H_DATE;
    };

    struct HistoryValue {
        // History Amount, indicating the money spent in the history
        double H_AMOUNT;
        std::array<char, 25> H_DATA;

    };

    struct CustomerKey {
        uint32_t C_W_ID;
        uint32_t C_D_ID;
        uint32_t C_ID;

        std::string buildTableKey(const std::string& prefix) {
            std::stringstream ss;
            ss << prefix << "_" << C_W_ID << "_" << C_D_ID << "_" << C_ID;
            return ss.str();
        }
    };

    struct CustomerValue {
        std::array<char, 16> C_FIRST;
        std::array<char, 2> C_MIDDLE;
        std::array<char, 16> C_LAST;
        std::array<char, 20> C_STREET_1;
        std::array<char, 20> C_STREET_2;
        std::array<char, 20> C_CITY;
        std::array<char, 2> C_STATE;
        std::array<char, 9> C_ZIP;
        std::array<char, 16> C_PHONE;
        std::array<char, 2> C_CREDIT;
        uint64_t C_SINCE;
        double C_CREDIT_LIM;
        double C_DISCOUNT;
        double C_BALANCE;
        double C_YTD_PAYMENT;
        uint32_t C_PAYMENT_CNT;
        uint32_t C_DELIVERY_CNT;
        std::string C_DATA; // max size 500;

        bool deserializeFromString(std::string_view raw) {
            auto in = zpp::bits::in(raw);
            if(failure(in(*this))) {
                return false;
            }
            return true;
        }
    };

    struct CustomerNameIdxKey {
        uint32_t C_W_ID;
        uint32_t C_D_ID;
        std::array<char, 16> C_LAST;
    };

    struct CustomerNameIdxValue {
        uint32_t C_ID;
    };

    struct DistrictKey {
        uint32_t D_W_ID;
        uint32_t D_ID;
    };

    struct DistrictValue {
        std::array<char, 10> D_NAME;
        std::array<char, 20> D_STREET_1;
        std::array<char, 20> D_STREET_2;
        std::array<char, 20> D_CITY;
        std::array<char, 2> D_STATE;
        std::array<char, 9> D_ZIP;
        double D_TAX;
        double D_YTD;
        uint32_t D_NEXT_O_ID;
    };

    struct WarehouseKey {
        uint32_t W_ID;
    };

    struct WarehouseValue {
        std::array<char, 10> W_NAME;
        std::array<char, 20> W_STREET_1;
        std::array<char, 20> W_STREET_2;
        std::array<char, 20> W_CITY;
        std::array<char, 2> W_STATE;
        std::array<char, 9> W_ZIP;
        double W_TAX;
        double W_YTD;
    };

}