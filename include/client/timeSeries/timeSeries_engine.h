//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/timeSeries/core_workload.h"
#include "client/timeSeries/timeSeries_property.h"

namespace client::timeSeries {
    struct TimeSeriesFactory {
        static std::shared_ptr<core::Workload> CreateWorkload(const util::Properties &) {
            return std::make_shared<CoreWorkload>();
        }

        static std::unique_ptr<TimeSeriesProperties> CreateProperty(const util::Properties &n) {
            return TimeSeriesProperties::NewFromProperty(n);
        }
    };

    using TimeSeriesEngine = core::DefaultEngine<TimeSeriesFactory, TimeSeriesProperties>;
}
