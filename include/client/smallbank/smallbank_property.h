//
// Created by user on 23-5-24.
//

#pragma once

#include "common/property.h"
#include "yaml-cpp/yaml.h"
#include "glog/logging.h"
#include <thread>

namespace client::smallbank{
    class SMALLBANKProperties {
    public:
        constexpr static const auto SMALLBANK_PROPERTIES = "smallbank";

        // table names
        constexpr static const auto ACCOUNTS_TAB = "account";
        constexpr static const auto SAVING_TAB = "saving";
        constexpr static const auto CHECKING_TAB = "checking";

        constexpr static const auto BATCH_SIZE = "batch_size";

        // account information
        constexpr static const auto NUM_ACCOUNTS = "num_accounts";

        constexpr static const auto HOTSPOT_USE_FIXED_SIZE  = "hotspot_use_fixed_size";
        constexpr static const auto HOTSPOT_PERCENTAGE      = "hotspot_percentage"; // [0% - 100%]
        constexpr static const auto HOTSPOT_FIXED_SIZE      = 100;   // fixed number of tuples

        constexpr static const auto MAX_BALANCE = 50000;
        constexpr static const auto MIN_BALANCE = 10000;

        // execution config
        constexpr static const auto DEFAULT_THREAD = 4;

        static std::unique_ptr<SMALLBANKProperties> NewFromProperty(const util::Properties &n) {
            auto ret = std::unique_ptr<SMALLBANKProperties>(new SMALLBANKProperties(n.getCustomPropertiesOrPanic(SMALLBANK_PROPERTIES)));
            return ret;
        }

    public:
        SMALLBANKProperties(const SMALLBANKProperties& rhs) = default;

        SMALLBANKProperties(SMALLBANKProperties&& rhs) noexcept : n(rhs.n) { }

        static void SetSMALLBANKProperties(auto&& key, auto&& value) {
            auto* properties = util::Properties::GetProperties();
            auto node = properties->getCustomProperties(SMALLBANK_PROPERTIES);
            node[key] = value;
        }

    public:
        inline bool getBatchSize() const {
            return n[BATCH_SIZE].as<int>(400);
        }

        inline int getNumAccounts() const {
            return n[NUM_ACCOUNTS].as<uint64_t>(1000000);
        }

        inline auto getHotspotUseFixedSize() const {
            return n[HOTSPOT_USE_FIXED_SIZE].as<bool>(false);
        }

        inline bool getHotspotPercentage() const {
            return n[HOTSPOT_PERCENTAGE].as<int>(25);
        }

        inline bool getHotspotFixedSize() const {
            return n[HOTSPOT_FIXED_SIZE].as<int>(100);
        }

        inline bool getMaxBalance() const {
            return n[MAX_BALANCE].as<int>(50000);
        }

        inline bool getMinBalance() const {
            return n[MIN_BALANCE].as<int>(10000);
        }

    protected:
        explicit SMALLBANKProperties(const YAML::Node& node) :n(node) { }

    private:
        YAML::Node n;
    };
}