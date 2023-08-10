//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/ycsb/core_workload.h"
#include "client/ycsb/ycsb_property.h"

namespace client::ycsb {
    struct YCSBFactory {
        static std::shared_ptr<core::Workload> CreateWorkload(const util::Properties &) {
            return std::make_shared<CoreWorkload>();
        }

        static std::unique_ptr<YCSBProperties> CreateProperty(const util::Properties &n) {
            return YCSBProperties::NewFromProperty(n);
        }
    };

    using YCSBEngine = core::DefaultEngine<YCSBFactory, YCSBProperties>;
}
