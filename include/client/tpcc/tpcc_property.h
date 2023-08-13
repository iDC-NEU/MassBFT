//
// Created by user on 23-5-24.
//

#pragma once

#include "client/core/default_property.h"

namespace client::tpcc {
    class TPCCProperties : public core::BaseProperties<TPCCProperties> {
    public:
        constexpr static const auto PROPERTY_NAME = "tpcc";

        constexpr static const auto NEW_ORDER_PROPORTION_PROPERTY = "new_order_proportion";

        constexpr static const auto PAYMENT_PROPORTION_PROPERTY = "payment_proportion";

        // maps threads to local warehouses
        constexpr static const auto WAREHOUSE_LOCALITY_PROPERTY = "warehouse_locality";

        constexpr static const auto WAREHOUSE_COUNT_PROPERTY = "warehouse_count";

        constexpr static const auto ENABLE_PAYMENT_LOOKUP_PROPERTY = "enable_payment_lookup";

    public:
        inline bool getWarehouseLocality() const {
            return n[WAREHOUSE_LOCALITY_PROPERTY].as<bool>(false);
        }

        inline int getWarehouseCount() const {
            return n[WAREHOUSE_COUNT_PROPERTY].as<int>(1);
        }

        inline bool enablePaymentLookup() const {
            return n[ENABLE_PAYMENT_LOOKUP_PROPERTY].as<bool>(false);
        }

    public:
        struct Proportion {
            double newOrderProportion;
            double paymentProportion;
        };

        inline Proportion getProportion() const {
            Proportion p{};
            p.newOrderProportion = n[NEW_ORDER_PROPORTION_PROPERTY].as<double>(0);
            p.paymentProportion =  n[PAYMENT_PROPORTION_PROPERTY].as<double>(0);
            return p;
        }

        explicit TPCCProperties(const YAML::Node& node) :core::BaseProperties<TPCCProperties>(node) { }
    };

}