//
// Created by user on 23-8-9.
//
#include "client/tpcc/tpcc_workload.h"


namespace client::tpcc {
    NewOrder TPCCWorkload::buildNewOrder(const Context &context, int32_t W_ID,
                             Random &random) const {
        NewOrderQuery query;
        // W_ID is constant over the whole measurement interval
        query.W_ID = W_ID;
        // The district number (D_ID) is randomly selected within [1 ..
        // context.n_district] from the home warehouse (D_W_ID = W_ID).
        query.D_ID = random.uniform_dist(1, context.n_district);

        // The non-uniform random customer number (C_ID) is selected using the
        // NURand(1023,1,3000) function from the selected district number (C_D_ID =
        // D_ID) and the home warehouse number (C_W_ID = W_ID).

        query.C_ID = random.non_uniform_distribution(1023, 1, 3000);

        // The number of items in the order (ol_cnt) is randomly selected within [5
        // .. 15] (an average of 10).

        query.O_OL_CNT = random.uniform_dist(5, 15);

        int rbk = random.uniform_dist(1, 100);

        for (auto i = 0; i < query.O_OL_CNT; i++) {

            // A non-uniform random item number (OL_I_ID) is selected using the
            // NURand(8191,1,100000) function. If this is the last item on the order
            // and rbk = 1 (see Clause 2.4.1.4), then the item number is set to an unused value.

            bool retry;
            do {
                retry = false;
                query.INFO[i].OL_I_ID =
                        random.non_uniform_distribution(8191, 1, 100000);
                for (int k = 0; k < i; k++) {
                    if (query.INFO[k].OL_I_ID == query.INFO[i].OL_I_ID) {
                        retry = true;
                        break;
                    }
                }
            } while (retry);

            if (i == query.O_OL_CNT - 1 && rbk == 1) {
                query.INFO[i].OL_I_ID = 0;
            }

            // The first supplying warehouse number (OL_SUPPLY_W_ID) is selected as
            // the home warehouse 90% of the time and as a remote warehouse 10% of the
            // time.

            if (i == 0) {
                int x = random.uniform_dist(1, 100);
                if (x <= context.newOrderCrossPartitionProbability &&
                    context.partition_num > 1) {
                    int32_t OL_SUPPLY_W_ID = W_ID;
                    while (OL_SUPPLY_W_ID == W_ID) {
                        OL_SUPPLY_W_ID = random.uniform_dist(1, context.partition_num);
                    }
                    query.INFO[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
                } else {
                    query.INFO[i].OL_SUPPLY_W_ID = W_ID;
                }
            } else {
                query.INFO[i].OL_SUPPLY_W_ID = W_ID;
            }
            query.INFO[i].OL_QUANTITY = random.uniform_dist(1, 10);
        }

        return query;
    }
}
