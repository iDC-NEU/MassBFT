//
// Created by user on 23-5-24.
//

#pragma once

#include "common/property.h"
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"
#include <thread>

namespace client::tpcc {
    class TPCCProperties {
    public:
        constexpr static const auto TPCC_PROPERTIES = "tpcc";

        constexpr static const auto THREAD_COUNT_PROPERTY = "thread_count";

        constexpr static const auto TARGET_THROUGHPUT_PROPERTY = "target_throughput";

        constexpr static const auto NEW_ORDER_PROPORTION_PROPERTY = "new_order_proportion";

        constexpr static const auto PAYMENT_PROPORTION_PROPERTY = "payment_proportion";

        // maps threads to local warehouses
        constexpr static const auto WAREHOUSE_LOCALITY_PROPERTY = "warehouse_locality";

        constexpr static const auto WAREHOUSE_COUNT_PROPERTY = "warehouse_count";

        // The target number of operations to perform.
        constexpr static const auto OPERATION_COUNT_PROPERTY = "operation_count";

        constexpr static const auto USE_RANDOM_SEED = "use_random_seed";

        constexpr static const auto ENABLE_PAYMENT_LOOKUP_PROPERTY = "enable_payment_lookup";

        static std::unique_ptr<TPCCProperties> NewFromProperty(const util::Properties &n) {
            auto ret = std::unique_ptr<TPCCProperties>(new TPCCProperties(n.getCustomPropertiesOrPanic(TPCC_PROPERTIES)));
            return ret;
        }

    public:
        TPCCProperties(const TPCCProperties& rhs) = default;

        TPCCProperties(TPCCProperties&& rhs) noexcept : n(rhs.n) { }

        static void SetTPCCProperties(auto&& key, auto&& value) {
            auto* properties = util::Properties::GetProperties();
            auto node = properties->getCustomProperties(TPCC_PROPERTIES);
            node[key] = value;
        }

    public:
        auto getThreadCount() const {
            return n[THREAD_COUNT_PROPERTY].as<int>((int)std::thread::hardware_concurrency());
        }

        double getTargetTPSPerThread() const {
            auto target = n[TARGET_THROUGHPUT_PROPERTY].as<int>(1000);  // testing targetTPS=1000
            auto threadCount = getThreadCount();
            return ((double) target) / threadCount;
        }

        inline bool getWarehouseLocality() const {
            return n[WAREHOUSE_LOCALITY_PROPERTY].as<bool>(false);
        }

        inline int getWarehouseCount() const {
            return n[WAREHOUSE_COUNT_PROPERTY].as<int>(1);
        }

        inline auto getOperationCount() const {
            return n[OPERATION_COUNT_PROPERTY].as<uint64_t>(10000); // 10k for default
        }

        inline bool getUseRandomSeed() const {
            return n[USE_RANDOM_SEED].as<bool>(true);
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

    protected:
        explicit TPCCProperties(const YAML::Node& node) :n(node) { }

    private:
        YAML::Node n;
    };

}