//
// Created by user on 23-8-9.
//

#pragma once

#include "client/tpcc/tpcc_types.h"
#include "zpp_bits.h"

namespace client::tpcc::schema {
    struct warehouse_t {
        static constexpr int id = 0;
        struct key_t {
            Integer w_id;
        };

        Varchar<10> w_name;
        Varchar<20> w_street_1;
        Varchar<20> w_street_2;
        Varchar<20> w_city;
        Varchar<2> w_state;
        Varchar<9> w_zip;
        Numeric w_tax{};
        Numeric w_ytd{};
        // fix for contention; could be solved with contention split from
        // http://cidrdb.org/cidr2021/papers/cidr2021_paper21.pdf
        uint8_t padding[1024]{};
    };

    struct district_t {
        static constexpr int id = 1;
        struct key_t {
            Integer d_w_id;
            Integer d_id;
        };

        Varchar<10> d_name;
        Varchar<20> d_street_1;
        Varchar<20> d_street_2;
        Varchar<20> d_city;
        Varchar<2> d_state;
        Varchar<9> d_zip;
        Numeric d_tax{};
        Numeric d_ytd{};
        Integer d_next_o_id{};
    };

    struct customer_t {
        static constexpr int id = 2;
        struct key_t {
            Integer c_w_id;
            Integer c_d_id;
            Integer c_id;
        };

        Varchar<16> c_first;
        Varchar<2> c_middle;
        Varchar<16> c_last;
        Varchar<20> c_street_1;
        Varchar<20> c_street_2;
        Varchar<20> c_city;
        Varchar<2> c_state;
        Varchar<9> c_zip;
        Varchar<16> c_phone;
        Timestamp c_since{};
        Varchar<2> c_credit;
        Numeric c_credit_lim{};
        Numeric c_discount{};
        Numeric c_balance{};
        Numeric c_ytd_payment{};
        Numeric c_payment_cnt{};
        Numeric c_delivery_cnt{};
        Varchar<500> c_data;
    };

    struct customer_wdl_t {
        static constexpr int id = 3;
        struct key_t {
            Integer c_w_id{};
            Integer c_d_id{};
            Varchar<16> c_last;
            Varchar<16> c_first;
        };

        Integer c_id;
    };

    struct history_t {
        static constexpr int id = 4;
        struct key_t {
            Integer h_c_id{};
            Integer h_c_d_id{};
            Integer h_c_w_id{};
            Integer h_d_id{};
            Integer h_w_id{};
            Timestamp h_date{};
        };

        Numeric h_amount{};
        Varchar<24> h_data;
    };

    struct new_order_t {
        static constexpr int id = 5;
        struct key_t {
            Integer no_w_id;
            Integer no_d_id;
            Integer no_o_id;
        };
        // New Order DUMMY means that in the New-Order test transaction,
        // the actual order generation operation is not performed,
        // but only the execution of the simulated transaction,
        // thereby reducing the load and overhead of the database.
        Integer no_dummy;
    };

    struct order_t {
        static constexpr int id = 6;
        struct key_t {
            Integer o_w_id;
            Integer o_d_id;
            Integer o_id;
        };

        Integer o_c_id;
        Timestamp o_entry_d;
        Integer o_carrier_id;
        Numeric o_ol_cnt;
        Numeric o_all_local;
    };

    // Order-Status (WDC) With Delivery Confirmation
    struct order_wdc_t {
        static constexpr int id = 7;
        struct key_t {
            Integer o_w_id;
            Integer o_d_id;
            Integer o_c_id;
            Integer o_id;
        };
    };

    struct order_line_t {
        static constexpr int id = 8;
        struct key_t {
            Integer ol_w_id;
            Integer ol_d_id;
            Integer ol_o_id;
            Integer ol_number;
        };

        Integer ol_i_id{};
        Integer ol_supply_w_id{};
        Timestamp ol_delivery_d{};
        Numeric ol_quantity{};
        Numeric ol_amount{};
        Varchar<24> ol_dist_info;
    };

    struct item_t {
        static constexpr int id = 9;
        struct key_t {
            Integer i_id;
        };

        Integer i_im_id{};
        Varchar<24> i_name;
        Numeric i_price{};
        Varchar<50> i_data;
    };

    struct stock_t {
        static constexpr int id = 10;
        struct key_t {
            Integer s_w_id;
            Integer s_i_id;
        };

        Numeric s_quantity{};
        Varchar<24> s_dist_01;
        Varchar<24> s_dist_02;
        Varchar<24> s_dist_03;
        Varchar<24> s_dist_04;
        Varchar<24> s_dist_05;
        Varchar<24> s_dist_06;
        Varchar<24> s_dist_07;
        Varchar<24> s_dist_08;
        Varchar<24> s_dist_09;
        Varchar<24> s_dist_10;
        Numeric s_ytd{};
        Numeric s_order_cnt{};
        Numeric s_remote_cnt{};
        Varchar<50> s_data;
    };
}