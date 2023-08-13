//
// Created by user on 23-8-13.
//

#pragma once

#include "common/property.h"
#include <thread>

namespace client::core {
    template<class Derived>
    class BaseProperties {
    public:
        // use how many threads as client worker
        constexpr static const auto THREAD_COUNT_PROPERTY = "thread_count";

        constexpr static const auto TARGET_THROUGHPUT_PROPERTY = "target_throughput";

        constexpr static const auto BENCHMARK_SECONDS_PROPERTY = "benchmark_seconds";

        // Use random seed to init client
        constexpr static const auto USE_RANDOM_SEED = "use_random_seed";

    public:
        static std::unique_ptr<Derived> NewFromProperty(const util::Properties &n) {
            const auto& name = BaseProperties::GetPropertyName();
            return std::unique_ptr<Derived>(new Derived(n.getCustomPropertiesOrPanic(name)));
        }

        constexpr static inline const auto& GetPropertyName() {
            return Derived::PROPERTY_NAME;
        }

    public:
        BaseProperties(const BaseProperties& rhs) = default;

        BaseProperties(BaseProperties&& rhs) noexcept : n(rhs.n) { }

        virtual ~BaseProperties() = default;

        static void SetProperties(auto&& key, auto&& value) {
            const auto& name = BaseProperties::GetPropertyName();
            auto* properties = util::Properties::GetProperties();
            auto node = properties->getCustomProperties(name);
            node[key] = value;
        }

        auto getThreadCount() const {
            return n[THREAD_COUNT_PROPERTY].as<int>((int)std::thread::hardware_concurrency());
        }

        auto getTargetThroughput() const {
            return n[TARGET_THROUGHPUT_PROPERTY].as<int>(1000);  // testing targetTPS=1000
        }

        inline auto getBenchmarkSeconds() const {
            return n[BENCHMARK_SECONDS_PROPERTY].as<uint64_t>(30);
        }

        inline bool getUseRandomSeed() const {
            return n[USE_RANDOM_SEED].as<bool>(true);
        }

    protected:
        explicit BaseProperties(const YAML::Node& node) :n(node) { }

    protected:
        YAML::Node n;
    };
}