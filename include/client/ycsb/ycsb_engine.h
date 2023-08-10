//
// Created by user on 23-6-28.
//

#pragma once

#include "client/core/default_engine.h"
#include "client/ycsb/core_workload.h"
#include "client/ycsb/ycsb_property.h"

namespace client::ycsb {
    class YCSBEngine : public core::DefaultEngine<YCSBEngine, YCSBProperties> {
    public:
        explicit YCSBEngine(const util::Properties &n)
                : core::DefaultEngine<YCSBEngine, YCSBProperties>(n) { }

        ~YCSBEngine() { waitUntilFinish(); }

        static auto CreateWorkload(const util::Properties &) {
            return std::make_shared<CoreWorkload>();
        }

        static auto CreateProperty(const util::Properties &n) {
            return YCSBProperties::NewFromProperty(n);
        }
    };
}
