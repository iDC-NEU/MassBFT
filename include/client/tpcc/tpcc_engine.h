//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/tpcc/tpcc_workload.h"
#include "client/tpcc/tpcc_property.h"

namespace client::tpcc {
    class TPCCEngine : public core::DefaultEngine<TPCCEngine, TPCCProperties> {
    public:
        explicit TPCCEngine(const util::Properties &n)
                : core::DefaultEngine<TPCCEngine, TPCCProperties>(n) { }

        ~TPCCEngine() { waitUntilFinish(); }

        static auto CreateWorkload(const util::Properties &) {
            return std::make_shared<TPCCWorkload>();
        }

        static auto CreateProperty(const util::Properties &n) {
            return TPCCProperties::NewFromProperty(n);
        }
    };
}
