//
// Created by user on 23-5-24.
//

#pragma once

#include "client/core/default_property.h"

namespace client::small_bank {
    class SmallBankProperties : public core::BaseProperties<SmallBankProperties> {
    public:
        constexpr static const auto PROPERTY_NAME = "small_bank";

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
        constexpr static const auto TRANSACT_SAVING_PROPORTION = "ts_proportion";
        constexpr static const auto AMALGAMATE_PROPORTION = "amg_proportion";
        constexpr static const auto WRITE_CHECK_PROPORTION = "wc_proportion";

    public:
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

        explicit SmallBankProperties(const YAML::Node& node) : core::BaseProperties<SmallBankProperties>(node) { }
    };
}