//
// Created by user on 23-5-24.
//

#pragma once

#include "common/property.h"

namespace client::small_bank {
    class SmallBankProperties {
    public:
        constexpr static const auto SMALL_BANK_PROPERTIES = "small_bank";

        // client worker count
        constexpr static const auto THREAD_COUNT_PROPERTY = "thread_count";

        constexpr static const auto TARGET_THROUGHPUT_PROPERTY = "target_throughput";

        // The target number of operations to perform.
        constexpr static const auto OPERATION_COUNT_PROPERTY = "operation_count";

        constexpr static const auto USE_RANDOM_SEED = "use_random_seed";

        // account information
        constexpr static const auto ACCOUNTS_COUNT_PROPERTY = "accounts_count";

        // Probability that accounts are chosen from the hotspot
        constexpr static const auto PROB_ACCOUNT_HOTSPOT = "prob_account_hotspot";
        // Percentage-based hotspot
        constexpr static const auto HOTSPOT_PERCENTAGE = "hotspot_percentage";
        constexpr static const auto HOTSPOT_USE_FIXED_SIZE = "hotspot_use_fixed_size";
        // fixed number of tuples, default 100
        constexpr static const auto HOTSPOT_FIXED_SIZE = "hotspot_fixed_size";

        constexpr static const auto BALANCE_PROPORTION = "bal_proportion";
        constexpr static const auto DEPOSIT_CHECKING_PROPORTION = "dc_proportion";
        constexpr static const auto TRANSACT_SAVING_PROPORTION = "rs_proportion";
        constexpr static const auto AMALGAMATE_PROPORTION = "ang_proportion";
        constexpr static const auto WRITE_CHECK_PROPORTION = "wc_proportion";

        static std::unique_ptr<SmallBankProperties> NewFromProperty(const util::Properties &n) {
            auto ret = std::unique_ptr<SmallBankProperties>(new SmallBankProperties(n.getCustomPropertiesOrPanic(SMALL_BANK_PROPERTIES)));
            return ret;
        }

    public:
        SmallBankProperties(const SmallBankProperties& rhs) = default;

        SmallBankProperties(SmallBankProperties&& rhs) noexcept : n(rhs.n) { }

        static void SetSmallBankProperties(auto&& key, auto&& value) {
            auto* properties = util::Properties::GetProperties();
            auto node = properties->getCustomProperties(SMALL_BANK_PROPERTIES);
            node[key] = value;
        }

    public:
        inline auto getThreadCount() const {
            return n[THREAD_COUNT_PROPERTY].as<int>((int)std::thread::hardware_concurrency());
        }

        inline auto getTargetTPSPerThread() const {
            auto target = n[TARGET_THROUGHPUT_PROPERTY].as<int>(1000);  // testing targetTPS=1000
            auto threadCount = getThreadCount();
            return ((double) target) / threadCount;
        }

        inline auto getOperationCount() const {
            return n[OPERATION_COUNT_PROPERTY].as<uint64_t>(10000); // 10k for default
        }

        inline bool getUseRandomSeed() const {
            return n[USE_RANDOM_SEED].as<bool>(true);
        }

        inline auto getAccountsCount() const {
            return n[ACCOUNTS_COUNT_PROPERTY].as<int>(1000000); // default 1M
        }

        inline auto getProbAccountHotspot() const {
            return n[PROB_ACCOUNT_HOTSPOT].as<double>(0.0);
        }

        inline auto getHotspotPercentage() const {
            auto ret = n[HOTSPOT_PERCENTAGE].as<double>(0.25);
            CHECK(ret >=0 && ret <=1) << "input HOTSPOT_PERCENTAGE out of range!";
            return ret;
        }

        inline auto hotspotUseFixedSize() const {
            return n[HOTSPOT_USE_FIXED_SIZE].as<bool>(false);
        }

        inline auto getHotspotFixedSize() const {
            return n[HOTSPOT_FIXED_SIZE].as<int>(100);
        }

    public:
        struct Proportion {
            double balProportion;
            double dcProportion;
            double tsProportion;
            double amgProportion;
            double wcProportion;
        };

        inline Proportion getProportion() const {
            Proportion p{};
            p.balProportion = n[BALANCE_PROPORTION].as<double>(0);
            p.dcProportion =  n[DEPOSIT_CHECKING_PROPORTION].as<double>(0);
            p.tsProportion =  n[TRANSACT_SAVING_PROPORTION].as<double>(0);
            p.amgProportion =  n[AMALGAMATE_PROPORTION].as<double>(0);
            p.wcProportion =  n[WRITE_CHECK_PROPORTION].as<double>(0);
            return p;
        }

    protected:
        explicit SmallBankProperties(const YAML::Node& node) : n(node) { }

    private:
        YAML::Node n;
    };
}