//
// Created by user on 23-8-9.
//

#include <memory>

namespace client::tpcc {
    struct NewOrder {
        // W_ID is constant over the whole measurement interval
        int32_t W_ID;
        // The district number (D_ID) is randomly selected within [1, context.n_district]
        // from the home warehouse (D_W_ID = W_ID).
        int32_t D_ID;
        // The non-uniform random customer number (C_ID) is selected using the
        // NURand(1023,1,3000) function from the selected district number (C_D_ID =
        // D_ID) and the home warehouse number (C_W_ID = W_ID).
        int32_t C_ID;
        // The number of items in the order (ol_cnt) is randomly selected within [5
        // .. 15] (an average of 10).
        int8_t O_OL_CNT;

        struct NewOrderInfo {
            int32_t OL_I_ID;
            int32_t OL_SUPPLY_W_ID;
            int8_t OL_QUANTITY;
        };

        NewOrderInfo INFO[15];
    };
}