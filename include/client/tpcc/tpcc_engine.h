//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/tpcc/tpcc_workload.h"
#include "client/tpcc/tpcc_property.h"

namespace client::tpcc {
    struct TPCCFactory {
        static std::shared_ptr<core::Workload> CreateWorkload(const util::Properties &) {
            return std::make_shared<TPCCWorkload>();
        }

        static std::unique_ptr<TPCCProperties> CreateProperty(const util::Properties &n) {
            return TPCCProperties::NewFromProperty(n);
        }
    };

    using TPCCEngine = core::DefaultEngine<TPCCFactory, TPCCProperties>;
}
