//
// Created by user on 23-8-9.
//

#pragma once

namespace client::tpcc {
    enum class WorkloadType { NEW_ORDER_ONLY, PAYMENT_ONLY, MIXED };

    struct Context {
        [[nodiscard]] static Context NewSinglePartitionContext(const Context& rhs) {
            Context c = rhs;
            c.new_order_cross_partition_probability = 0;
            c.payment_cross_partition_probability = 0;
            return c;
        }

        [[nodiscard]] static Context NewCrossPartitionContext(const Context& rhs) {
            Context c = rhs;
            c.new_order_cross_partition_probability = 100;
            c.payment_cross_partition_probability = 100;
            return c;
        }

        WorkloadType workloadType = WorkloadType::NEW_ORDER_ONLY;
        // number of district
        int n_district = 10;
        // out of 100
        int new_order_cross_partition_probability = 10;
        // out of 100
        int payment_cross_partition_probability = 15;
        // by default, we run standard tpc-c.
        bool write_to_w_ytd = true;
        // look up C_ID on secondary index.
        bool payment_look_up = false;
    };
}